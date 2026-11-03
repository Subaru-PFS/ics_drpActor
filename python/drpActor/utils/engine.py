import datetime
import os
from datetime import timezone
from importlib import reload
import time

import drpActor.utils.dotRoach as dotRoach

reload(dotRoach)

from lsst.daf.butler import Butler
import drpActor.utils.catchUp as catchUp
from drpActor.utils.files import visitFiles
from drpActor.utils.pfsVisit import PfsVisit
from drpActor.utils.tasks.ingest import IngestHandler
from lsst.pipe.base import Pipeline
from drpActor.utils.chainedCollection import prepend_collection_chain
from drpActor.utils.reduceProcess import runInChild
from drpActor.utils.threading import Worker
from ics.utils.opdb import opDB
from lsst.daf.butler.cli.cliLog import CliLog

# Set INFO level logging for drpActor on module import.
CliLog.setLogLevels([('lsst', 'INFO'), ('pfs', 'INFO')])
CliLog.initLog(longlog=True)


def groupConfigOverride(expType, sequenceType):
    """
    Config overrides for reducing a group of visits sharing the same exposure type.

    Parameters
    ----------
    expType : str
        sps_visit.exp_type of the visits (e.g. 'object', 'arc', 'flat', 'dark', 'bias', 'test').
    sequenceType : str
        Type of the IIC sequence the visits belong to (e.g. 'scienceObject', 'scienceObject_windowed').

    Returns
    -------
    configOverride : dict
        {taskLabel: {configKey: value}}.
    """
    # opdb does not record windowing, only the sequenceType does.
    requireAdjustDetectorMap = expType == 'object' and not sequenceType.endswith('_windowed')
    quickCDS = expType not in ['object', 'dark']

    return dict(reduceExposure={'requireAdjustDetectorMap': requireAdjustDetectorMap},
                isr={'h4.quickCDS': quickCDS})


class DrpEngine:
    """
    Engine responsible for data reduction, ingestion, and configuration management for PFS.

    This class manages raw data ingestion, reduction pipelines, and configurations
    for the Prime Focus Spectrograph (PFS) instrument.

    Parameters
    ----------
    actor : object
        Actor instance responsible for logging and configuration management.
    datastore : str
        Path to the datastore where input/output collections are stored.
    rawRun : str
        Run name for raw data.
    pfsConfigRun : str
        Run name for pfsConfig.
    inputCollection : str
        Input collection (chained or tagged) used for reduction.
    outputCollection : str
        Output collection where task products are written.
    ingestMode : str
        Ingestion mode ("link" or "copy").
    pipelineYaml : str
        Path to the pipeline YAML (tasks/configs/contracts), typically under $PFS_INSTDATA_DIR/config.
    groupVisit : bool
        If True, reduce visits as groups; otherwise, reduce each visit independently.
    fail_fast : bool
        Abort pipeline execution on first failing quantum (equivalent to pipetask --fail-fast).
    numProc : int
        Number of worker processes (quanta executed in parallel).
    taskThreads : int
        Threads per quantum (ExecutionResources.num_cores); usually keep at 1.
    clobberOutput : bool
        If True, allow overwriting existing outputs in the run.
    timeout : dict or None
        Keyword arguments of reductionTimeout ({"base": seconds, "perVisit": seconds}), a reduction running longer
        than its visits allow being killed, None to never kill it.
    lookback : float
        Hours catchUpVisits looks back.
    catchUpDelay : float
        Seconds between a connection to the hub and the catch-up it triggers.
    replayWindow : float
        Seconds after a connection to the hub during which exposure, visit and sequence events are ignored, as
        replayed values.
    lsstLog : dict
        LSST logging configuration (e.g., {"level": 10, "long_log": False}).
    detrendCallback : dict
        Detrend-key callback configuration (e.g., {"activated": False, "useSilentThread": True,
        "waitInterval": 1, "timeout": 90}).

    Notes
    -----
    - `numProc` controls **process** parallelism at the executor level, in the reduction's child process.
    - `taskThreads` controls **per-quantum** threading; keep 1 unless explicitly tuned.
    - When fail_fast is True, the first task failure stops further execution in the current run.
    """

    def __init__(self, actor, datastore, rawRun, pfsConfigRun, inputCollection, outputCollection, ingestMode,
                 pipelineYaml, groupVisit, fail_fast, numProc, taskThreads, clobberOutput, lsstLog, detrendCallback,
                 timeout=None, lookback=24, catchUpDelay=120, replayWindow=5):
        """Lightweight init; heavy setup happens in dedicated methods."""
        self.actor = actor  # actor-provided logger/config access
        self.datastore = datastore  # butler repo root/URI
        self.rawRun = rawRun  # run for raw exposures
        self.pfsConfigRun = pfsConfigRun  # run for PFS config datasets
        self.inputCollection = inputCollection  # read collection for pipeline inputs
        self.outputCollection = outputCollection  # write collection for pipeline outputs
        self.ingestMode = ingestMode  # ingestion policy selector
        self.pipelineYaml = pipelineYaml  # pipeline yaml file path
        self.groupVisit = groupVisit  # reduce visits as a group.
        self.fail_fast = fail_fast  # run pipeline in fail_fast mode.

        # execution/logging/callback options
        self.numProc = numProc  # number of worker processes (process-level parallelism)
        self.taskThreads = taskThreads
        self.clobberOutput = clobberOutput
        self.timeout = timeout
        self.lookback = lookback
        self.catchUpDelay = catchUpDelay
        self.replayWindow = replayWindow
        self.replayEnds = 0  # time.monotonic() until which keyword events are ignored.
        self.lsstLog = lsstLog if lsstLog is not None else {}
        self.detrendCallback = detrendCallback
        self.doGenDetrendKey = detrendCallback.get('activated', False)

        self.pfsVisits = {}  # visitId -> list of exposure ids
        self.rawButler = None  # butler for raw/ingest operations
        self.dotRoach = None
        self.configOverride = None  # no config override yet.
        self.reductionQueued = set()  # visits queued on the reduction thread, or being reduced.

        # Enable auto-ingest and auto-reduction by default
        self.doAutoIngest = True
        self.doAutoReduce = True

        # Initialize Butler instances and handlers
        self.rawButler = self.loadButler(self.rawRun)
        self.pfsConfigButler = self.loadButler(self.pfsConfigRun)
        self.butler = Butler(self.datastore, collections=[self.inputCollection, self.outputCollection])

        self.ingestHandler = IngestHandler(self)
        self.reducePipeline, self.timestamp = self.setupReducePipeline(datastore, outputCollection, pipelineYaml)

        # ingest and reduction each run in their own thread, in the order they are queued.
        self.ingestThread = Worker(actor, 'ingest')
        self.reduceThread = Worker(actor, 'reduce')
        self.condaEnv = os.environ.get("CONDA_DEFAULT_ENV")

    @property
    def logger(self):
        """Retrieve the logger instance from the actor."""
        return self.actor.logger

    @classmethod
    def fromConfigFile(cls, actor):
        """Create a DrpEngine instance from the actor's configuration file."""
        # loading per-site config
        siteConfig = actor.actorConfig[actor.site].get('engine')

        # getting datastore and collections
        butler = siteConfig.get('butler')
        datastore = butler.get('datastore')
        rawRun = butler.get('raw')
        pfsConfigRun = butler.get('pfsConfig')
        inputCollection = butler.get('input')
        outputCollection = butler.get('output')

        # ingest config
        ingest = siteConfig.get('ingest')
        ingestMode = ingest.get('mode')

        # pipeline config
        pipeline = siteConfig.get('pipeline')
        pipelineYaml = pipeline.get('yaml')
        groupVisit = pipeline.get('groupVisit')
        fail_fast = pipeline.get('fail_fast')

        # execution, numProc
        execution = siteConfig.get('execution')
        numProc = execution.get('numProc')
        taskThreads = execution.get('taskThreads')
        clobberOutput = execution.get('clobberOutput')
        timeout = execution.get('timeout')

        # catch-up
        catchUp = siteConfig.get('catchUp', {})
        lookback = catchUp.get('lookback', 24)
        catchUpDelay = catchUp.get('delay', 120)
        replayWindow = catchUp.get('replayWindow', 5)

        # opdb
        opdbHost = siteConfig.get('opdb').get('host')
        opDB.host = opdbHost

        # logs and callbacks
        lsstLog = siteConfig.get('lsstLog')
        detrendCallback = siteConfig.get('detrendCallback')

        return cls(actor,
                   datastore=datastore,
                   rawRun=rawRun,
                   pfsConfigRun=pfsConfigRun,
                   inputCollection=inputCollection,
                   outputCollection=outputCollection,
                   ingestMode=ingestMode,
                   pipelineYaml=pipelineYaml,
                   groupVisit=groupVisit,
                   fail_fast=fail_fast,
                   numProc=numProc,
                   taskThreads=taskThreads,
                   clobberOutput=clobberOutput,
                   lsstLog=lsstLog,
                   detrendCallback=detrendCallback,
                   timeout=timeout,
                   lookback=lookback,
                   catchUpDelay=catchUpDelay,
                   replayWindow=replayWindow)

    def loadButler(self, run):
        """
        Initialize a Butler instance for a specific run.

        Parameters
        ----------
        run : str
            The run identifier to initialize the Butler.

        Returns
        -------
        Butler or None
            The initialized Butler instance or None if initialization fails.
        """
        try:
            return Butler(self.datastore, run=run)
        except Exception as e:
            self.logger.warning('Failed to load Butler: %s', self.actor.strTraceback(e))
            return None

    def setupReducePipeline(self, datastore, chainedCollection, pipelineYaml):
        """
        Load the reduction pipeline and register a new timestamped output run at the head of the chain.

        Parameters
        ----------
        datastore : str
            Path to the datastore (Butler repo root/URI).
        chainedCollection : str
            Name of the output chained collection (base of the run).
        pipelineYaml : str
            Path (relative to $PFS_INSTDATA_DIR/config) to the pipeline YAML.

        Returns
        -------
        tuple
            (pipeline, timestamp), the run being chainedCollection/timestamp.
        """
        pipeline = Pipeline.fromFile(self.pipelinePath(pipelineYaml))

        # Append a timestamp to the output collection name
        timestamp = datetime.datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        run = os.path.join(chainedCollection, timestamp)

        # a butler created with a run registers it.
        Butler(datastore, run=run)

        # new run takes precedence over the previous ones.
        prepend_collection_chain(datastore, chainedCollection, run, logger=self.logger)

        return pipeline, timestamp

    @staticmethod
    def pipelinePath(pipelineYaml):
        """Full path of a pipeline YAML given relative to $PFS_INSTDATA_DIR/config."""
        return os.path.join(os.getenv("PFS_INSTDATA_DIR"), 'config', pipelineYaml)

    def stop(self):
        """Stop the ingest and reduction threads once their queued work is done."""
        self.ingestThread.exit()
        self.reduceThread.exit()

    def isReplay(self, event):
        """Whether a keyword event comes within replayWindow of a connection to the hub, logging it if so."""
        if time.monotonic() >= self.replayEnds:
            return False

        self.logger.info(f'ignoring replayed {event}')
        return True

    def onPfsConfig(self, pfsConfigFile):
        """
        Queue a new pfsConfig file on the ingest thread, see newPfsConfig.

        Kept when replayed on connection, the last pfsConfig being often the one of the next visit.
        """
        self.ingestThread.putMsg(self.newPfsConfig, pfsConfigFile=pfsConfigFile)

    def onExposure(self, exposureFile):
        """Queue a new exposure file on the ingest thread, see newExposure."""
        if self.isReplay(f'exposure {exposureFile.filepath}'):
            return

        self.ingestThread.putMsg(self.newExposure, exposureFile=exposureFile)

    def onVisit(self, visit, delay=0.2):
        """
        Queue a finished visit on the ingest thread, see newVisit.

        The delay lets the keywords of the visit's last files, which come from other actors, be queued first.
        """
        if self.isReplay(f'visit {visit}'):
            return

        self.ingestThread.putMsgLater(delay, self.newVisit, visit=visit)

    def onVisitGroup(self, sequenceId, groupId, sequenceType, name, comments, cmdStr, status, output, delay=0.5):
        """
        Queue a finished IIC sequence on the ingest thread, see newVisitGroup.

        The delay lets the sequence's last visit be queued first.
        """
        if self.isReplay(f'iic_sequence {sequenceId}'):
            return

        self.ingestThread.putMsgLater(delay, self.newVisitGroup, sequenceId=sequenceId, groupId=groupId,
                                      sequenceType=sequenceType, name=name, comments=comments, cmdStr=cmdStr,
                                      status=status, output=output)

    def newPfsConfig(self, pfsConfigFile):
        """
        Register a new PFS configuration file for a visit.

        Parameters
        ----------
        pfsConfigFile : object
            The PFS configuration file to be registered.
        """
        self.logger.info(f'New pfsConfig available: {pfsConfigFile.filepath}')
        pfsConfigFile.initialize(self.pfsConfigButler)

        self.pfsVisits[pfsConfigFile.visit] = PfsVisit(pfsConfigFile.visit, pfsConfigFile=pfsConfigFile)

    def newExposure(self, exposureFile):
        """
        Add a new exposure file to the corresponding visit.

        Parameters
        ----------
        exposureFile : object
            The exposure file to be added to a visit.
        """
        self.logger.info(f'New exposure available: {exposureFile.filepath}')
        exposureFile.initialize(self.rawButler)

        if exposureFile.visit not in self.pfsVisits:
            self.logger.warning(f'No pfsVisit found for visit {exposureFile.visit}')
            self.pfsVisits[exposureFile.visit] = PfsVisit(exposureFile.visit)

        self.pfsVisits[exposureFile.visit].addExposure(exposureFile)

    def newVisit(self, visit):
        """
        Process a new visit by ingesting exposures and configurations.

        Parameters
        ----------
        visit : int
            Identifier for the visit to be processed.
        """
        pfsVisit = self.pfsVisits.get(visit)

        if not pfsVisit:
            self.logger.warning(f'No pfsVisit found for visit {visit}')
            return

        self.processPfsVisit(pfsVisit)

    def newVisitGroup(self, sequenceId, groupId, sequenceType, name, comments, cmdStr, status, output):
        """
        Queue the group reduction of a finished IIC sequence when groupVisit, see reduceSequence.

        Parameters
        ----------
        sequenceId : int or str
            iic_sequence.iic_sequence_id. Cast to int internally.
        groupId : int or str
            Group identifier from IIC, logged only.
        sequenceType : str
            Type of the IIC sequence (e.g. 'scienceObject', 'arc', 'flat'); see groupConfigOverride.
        name : str
            Human-readable sequence name from IIC, logged only.
        comments : str
            Free-form comment string associated with the sequence, logged only.
        cmdStr : str
            Original IIC command string that created the sequence, not interpreted.
        status : str
            Sequence status as reported by IIC, not interpreted.
        output : dict or Any
            Additional output payload from IIC, not interpreted.
        """
        try:
            sequenceId = int(sequenceId)
        except Exception:
            self.logger.warning(f'Invalid sequenceId {sequenceId!r}')
            return

        self.logger.info(f'New iic_sequence sequenceId={sequenceId} groupId={groupId} '
                         f'sequenceType={sequenceType} name={name!r} comments={comments!r}')

        if self.groupVisit:
            self.reduceSequence(sequenceId, sequenceType=str(sequenceType))

    def reduceSequence(self, sequenceId, sequenceType=None, force=False, cmd=None):
        """
        Queue the reduction of an IIC sequence's visits as one group, ingesting first the ones the butler lacks.

        The group is skipped with a warning if any of its visits cannot be fully ingested, or, unless force, if all
        its visits are already reduced or queued for reduction. Visits sharing a single sps_visit.exp_type are
        reduced with the config overrides given by groupConfigOverride, a group mixing exp_types with the pipeline
        defaults.

        Parameters
        ----------
        sequenceId : int
            iic_sequence.iic_sequence_id.
        sequenceType : str, optional
            iic_sequence.sequence_type, looked up in opdb if None.
        force : bool
            Reduce the group even if all its visits are already reduced.
        cmd : actorcore.Command, optional
            Command receiving the statuses and finished with the outcome of the reduction.

        Returns
        -------
        queued : bool
            True if the reduction was queued.
        """

        def skip(text):
            self.logger.warning(f'visitGroup ({sequenceId}) : {text}')
            if cmd is not None:
                cmd.fail(f'text="visitGroup ({sequenceId}) : {text}"')
            return False

        expTypes = catchUp.sequenceVisits(sequenceId)

        if not expTypes:
            return skip('no SPS visit')

        if sequenceType is None:
            sequenceType = catchUp.sequenceType(sequenceId)

        visits = sorted(expTypes)
        butler = self.queryButler()
        taken = catchUp.visitExposures(visits)
        missing = self.missingData(butler, taken)

        if missing:
            for visit in sorted(missing):
                self.ingestVisit(visit, cmd=cmd)

            missing = self.missingData(butler, taken)

        if missing:
            return skip(f'not ingested {missing}')

        if not force:
            done = catchUp.reducedVisits(butler, self.outputCollection, visits) | self.reductionQueued

            if set(visits) <= done:
                self.logger.info(f'visitGroup ({sequenceId}) : already reduced or queued')
                if cmd is not None:
                    cmd.finish(f'text="visitGroup ({sequenceId}) : already reduced or queued"')
                return False

        uniqueExpTypes = sorted(set(expTypes.values()))
        configOverride = None

        if len(uniqueExpTypes) == 1:
            configOverride = groupConfigOverride(uniqueExpTypes[0], str(sequenceType))
        else:
            self.logger.warning(f'visitGroup ({sequenceId}) : mixed exp_type {uniqueExpTypes}, '
                                f'reducing without config override')

        self.reductionQueued.update(visits)
        self.reduceThread.putMsg(self.reduceVisitGroup, visits=visits, configOverride=configOverride, cmd=cmd)
        return True

    def processPfsVisit(self, pfsVisit):
        """
        Ingest a single PFS visit, queue its reduction and run the optional tools.

        Parameters
        ----------
        pfsVisit : PfsVisit
            The visit object containing exposures and configurations.
        """
        if self.doAutoIngest:
            self.ingestHandler.doIngest(pfsVisit)

        if pfsVisit.isIngested and not self.groupVisit:
            if self.doAutoReduce and self.isUnreduced(pfsVisit.visit):
                self.reductionQueued.add(pfsVisit.visit)
                self.reduceThread.putMsg(self.reduceVisit, pfsVisit=pfsVisit)

            # run roaches ! run !
            if self.dotRoach is not None:
                self.dotRoach.run(pfsVisit)

        # Just declaring that visit should be processed, just a placeholder for now.
        pfsVisit.finish()

    def isUnreduced(self, visit):
        """Whether a visit is neither reduced, see catchUp.reducedVisits, nor queued for reduction."""
        if visit in self.reductionQueued:
            return False

        return not catchUp.reducedVisits(self.queryButler(), self.outputCollection, [visit])

    def queryButler(self):
        """A new butler on the datastore, for registry queries."""
        return Butler(self.datastore)

    def onConnect(self):
        """
        Ignore the exposure, visit and sequence values the hub replays on connection, and queue a catch-up covering
        them.

        The keyword values sent while disconnected are lost, and the replayed ones may describe a visit only in part.
        The catch-up waits catchUpDelay so that the exposures ending around the connection are past its settling
        time, see catchUpVisits.
        """
        self.replayEnds = time.monotonic() + self.replayWindow
        self.ingestThread.putMsgLater(self.catchUpDelay, self.catchUpVisits)

    def catchUpVisits(self, lookback=None, cmd=None):
        """
        Ingest the recent exposures the butler lacks, then queue the reduction of the visits not reduced yet.

        The exposures opdb lists for the last hours are compared with the butler's raw and pfsConfig datasets, see
        missingData, the visits lacking any being ingested from /data/raw. The fully ingested visits holding no reduction, see catchUp.reducedVisits, and not
        queued for one are then reduced if doAutoReduce: by finished IIC sequence when groupVisit, see
        reduceSequence, one by one otherwise.

        Parameters
        ----------
        lookback : float, optional
            Hours to look back, self.lookback if None.
        cmd : actorcore.Command, optional
            Command receiving the statuses and finished with a summary, the actor's bcast if None.
        """
        lookback = self.lookback if lookback is None else lookback
        butler = self.queryButler()

        taken = catchUp.takenExposures(lookback)
        missing = self.missingData(butler, taken)

        for visit, items in sorted(missing.items()):
            self.logger.info(f'catchUp: visit {visit} missing {items}')
            self.ingestVisit(visit, cmd=cmd)

        if missing:
            missing = self.missingData(butler, taken)

            if missing:
                self.logger.warning(f'catchUp: could not ingest {sorted(missing)}')

        nQueued = 0

        if self.doAutoReduce:
            complete = set(taken) - set(missing)
            unreduced = complete - catchUp.reducedVisits(butler, self.outputCollection, complete) - self.reductionQueued

            if self.groupVisit:
                for sequenceId, sequenceType in sorted(catchUp.finishedSequences(unreduced).items()):
                    nQueued += self.reduceSequence(sequenceId, sequenceType=sequenceType)
            else:
                for visit in sorted(unreduced):
                    self.reductionQueued.add(visit)
                    self.reduceThread.putMsg(self.processVisitGroup, visits=[visit])
                    nQueued += 1

        text = (f'catchUp over {lookback}h: {len(taken)} visits, {len(missing)} left not ingested, '
                f'{nQueued} reductions queued')
        self.logger.info(text)

        if cmd is None:
            self.actor.bcast.inform(f'text="{text}"')
        else:
            cmd.finish(f'text="{text}"')

    def missingData(self, butler, taken):
        """
        Raw exposures and pfsConfig files the butler lacks.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            Butler.
        taken : dict
            {visit: {(arm, spectrograph)}} of the exposures expected.

        Returns
        -------
        missing : dict
            {visit: [item]} with items like 'b1', 'n3' or 'pfsConfig', visits lacking nothing being absent.
        """
        exposures = catchUp.missingExposures(taken, catchUp.ingestedExposures(butler, self.rawRun, taken))
        missing = {visit: sorted(f'{arm}{spectrograph}' for arm, spectrograph in cams)
                   for visit, cams in exposures.items()}

        for visit in sorted(set(taken) - catchUp.ingestedPfsConfigs(butler, self.pfsConfigRun, taken)):
            missing.setdefault(visit, []).append('pfsConfig')

        return missing

    def ingestVisit(self, visit, cmd=None):
        """
        Ingest the files of a visit found in /data/raw, see ingestFiles, logging a failure rather than raising it.

        Parameters
        ----------
        visit : int
            Visit.
        cmd : actorcore.Command, optional
            Command receiving the ingest status, the actor's bcast if None.
        """
        try:
            pfsConfigFile, exposureFiles = visitFiles(visit)
            self.ingestFiles(pfsConfigFile, exposureFiles, cmd=cmd)
        except Exception:
            self.logger.exception(f'visit {visit}: ingest failed')

    def ingestFiles(self, pfsConfigFile, exposureFiles, cmd=None):
        """
        Register and ingest the files of one visit.

        Parameters
        ----------
        pfsConfigFile : PfsConfigFile
            The visit's pfsConfig file.
        exposureFiles : list of PfsFile
            The visit's exposure files.
        cmd : actorcore.Command, optional
            Command receiving the ingest status, the actor's bcast if None.
        """
        self.newPfsConfig(pfsConfigFile)

        for exposureFile in exposureFiles:
            self.newExposure(exposureFile)

        self.ingestHandler.doIngest(self.pfsVisits[pfsConfigFile.visit], cmd=cmd)

    def reduceVisit(self, pfsVisit):
        """
        Reduce a single ingested visit, with the detrend callback if activated, see processVisitGroup.

        Parameters
        ----------
        pfsVisit : PfsVisit
            Visit to reduce.
        """
        # setting up a callback to be generated when the file is generated.
        if self.doGenDetrendKey:
            pfsVisit.setupDetrendCallback(self)

        self.processVisitGroup([pfsVisit.visit])

    def reduceVisitGroup(self, visits, configOverride=None, cmd=None):
        """
        Reduce a group of ingested visits with the given config overrides, see processVisitGroup.

        Parameters
        ----------
        visits : list of int
            Visits to reduce together.
        configOverride : dict, optional
            {taskLabel: {configKey: value}}, None for the pipeline defaults.
        cmd : actorcore.Command, optional
            Command finished with the outcome of the reduction.
        """
        if configOverride is not None:
            self.addConfigOverride(configOverride)
        # an empty override marks the run as holding the pipeline defaults, so the next override rolls a new run.
        elif self.configOverride:
            self.newReduceRun(configOverride={})
        else:
            self.addConfigOverride({})

        status = self.processVisitGroup(visits)

        if cmd is None:
            return

        if status != 'ok':
            cmd.fail(f'text="reduction {status}"')
            return

        cmd.finish()

    def reduceCommand(self, cmd, where, configOverride):
        """
        Reduce the data selected by a command, and finish the command with the outcome.

        Parameters
        ----------
        cmd : actorcore.Command
            Command to finish.
        where : str
            Data query selecting what to reduce.
        configOverride : dict
            {taskLabel: {configKey: value}}, merged into the active overrides.
        """
        if configOverride:
            self.addConfigOverride(configOverride)

        status = self.runReductionPipeline(where=where)

        if status != 'ok':
            cmd.fail(f'text="reduction {status}"')
            return

        cmd.finish()

    def processVisitGroup(self, visits):
        """
        Reduce a group of already ingested visits in one pipeline execution and report the outcome per visit.

        Parameters
        ----------
        visits : list of int
            Visits to process together.

        Returns
        -------
        status : str
            Outcome of the reduction, see runReductionPipeline.

        Notes
        -----
        - Process concurrency is controlled by self.numProc; per-quantum threading by self.taskThreads.
        - The pipeline outcome is reported in reduceExposureStatus for each visit.
        """
        visitStr = ','.join(str(visit) for visit in visits)

        self.logger.info(f'processVisitGroup started on {visitStr}')
        t0 = time.perf_counter()

        try:
            status = self.runReductionPipeline(where=f"visit in ({visitStr})")
        finally:
            self.reductionQueued.difference_update(visits)

        t1 = time.perf_counter()

        cmd = self.actor.bcast
        returnCode, statusStr = (0, 'OK') if status == 'ok' else (1, status.upper())

        for visit in visits:
            cmd.inform(f'reduceExposureStatus={visit},{returnCode},"{statusStr}",{t1 - t0:.1f}')

        return status

    def runReductionPipeline(self, where, retry=True):
        """
        Run the reduction pipeline on the selected data in the current run, in a child process, see runInChild.

        Parameters
        ----------
        where : str
            Data query selecting what to reduce.
        retry : bool
            Allow a single retry in a new run when the current run holds other software versions or task
            configs.

        Returns
        -------
        status : str
            'ok', 'failed', 'conflict', 'timeout' or 'crashed'.
        """
        run = os.path.join(self.outputCollection, self.timestamp)
        logDir = os.path.expandvars(self.actor.actorConfig['logging']['logdir'])
        now = datetime.datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        logFile = os.path.join(logDir, 'reductions', self.timestamp, f'{now}.log')

        self.logger.info(f'reduction where="{where}" num_proc={self.numProc} fail_fast={self.fail_fast} '
                         f'log={logFile}')

        status, message = runInChild(logFile, timeout=self.timeout, datastore=self.datastore,
                                     inputCollection=self.inputCollection, run=run,
                                     pipelinePath=self.pipelinePath(self.pipelineYaml), where=where,
                                     configOverride=self.configOverride, numProc=self.numProc,
                                     taskThreads=self.taskThreads, failFast=self.fail_fast,
                                     clobberOutput=self.clobberOutput)

        self.logger.info(f'reduction where="{where}" {status}')

        if status == 'conflict' and retry:
            # A run holds a single packages dataset and a single config per task, so a conda package updated
            # underneath the actor makes that run unusable, only a new one can record the new versions.
            self.logger.warning(f'Run holds other software versions or task configs: {message}')
            self.newReduceRun(configOverride=self.configOverride)
            return self.runReductionPipeline(where, retry=False)

        if status != 'ok':
            self.logger.warning(f'reduction where="{where}" {status}: {message}')

        return status

    def newReduceRun(self, configOverride=None):
        """
        Create a new output run and reload the reduction pipeline.

        Parameters
        ----------
        configOverride : dict, optional
            Overrides to apply in the new run, which starts without any.
        """
        self.logger.info('Creating new reduction run.')

        self.reducePipeline, self.timestamp = self.setupReducePipeline(self.datastore, self.outputCollection,
                                                                       self.pipelineYaml)
        self.configOverride = None

        if configOverride is not None:
            self.addConfigOverride(configOverride)

    def addConfigOverride(self, configOverride):
        """
        Merge config overrides into the active ones, creating a new run whenever that changes the current
        configuration.

        Parameters
        ----------
        configOverride : dict
            {taskLabel: {configKey: value}}, only those keys are overridden, the ones already set keep
            their current value.
        """
        merged = {label: dict(cfg) for label, cfg in (self.configOverride or {}).items()}

        for label, cfg in configOverride.items():
            merged.setdefault(label, {}).update(cfg)

        if merged == self.configOverride:
            return

        if self.configOverride is not None:
            self.logger.info('Config override changed.')
            self.newReduceRun(configOverride=merged)
            return

        for label, cfg in merged.items():
            for key, value in cfg.items():
                # can't add config override for non-defined task.
                if label not in self.reducePipeline.task_labels:
                    continue

                self.logger.info(f'config override:{label} {key}={value}')

        self.configOverride = merged

    def startDotRoach(self, dataRoot, maskFile, cams, keepMoving=False):
        """Starting dotRoach loop."""
        # Deactivating auto-detrend.
        self.doAutoReduce = False
        # instantiating DotRoach object.
        self.dotRoach = dotRoach.DotRoach(self, dataRoot, maskFile, cams, keepMoving=keepMoving)

    def stopDotRoach(self, cmd):
        """Stopping dotRoach loop"""
        # Re-activating auto-detrend.
        self.doAutoReduce = True

        if not self.dotRoach:
            cmd.warn('text="no dotRoach loop on-going."')
            return

        cmd.inform('text="ending dotRoach loop"')
        self.dotRoach.finish()
        self.dotRoach = None
