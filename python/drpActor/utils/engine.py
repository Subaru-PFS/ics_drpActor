import datetime
import os
from datetime import timezone
from importlib import reload
import time

import drpActor.utils.dotRoach as dotRoach

reload(dotRoach)

from lsst.daf.butler import Butler
from drpActor.utils.pfsVisit import PfsVisit
from drpActor.utils.tasks.ingest import IngestHandler
from lsst.pipe.base import Pipeline
from drpActor.utils.chainedCollection import prepend_collection_chain
from drpActor.utils.pipetask import pipetaskCommand, runPipetask, runIsInconsistent
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
        Cores per quantum (pipetask -n); usually keep at 1.
    clobberOutput : bool
        If True, allow overwriting existing outputs in the run.
    lsstLog : dict
        LSST logging configuration (e.g., {"level": 10, "long_log": False}).
    detrendCallback : dict
        Detrend-key callback configuration (e.g., {"activated": False, "useSilentThread": True,
        "waitInterval": 1, "timeout": 90}).

    Notes
    -----
    - `numProc` controls **process** parallelism (pipetask -j).
    - `taskThreads` controls **per-quantum** threading; keep 1 unless explicitly tuned.
    - When fail_fast is True, the first task failure stops further execution in the current run.
    """

    def __init__(self, actor, datastore, rawRun, pfsConfigRun, inputCollection, outputCollection, ingestMode,
                 pipelineYaml, groupVisit, fail_fast, numProc, taskThreads, clobberOutput, lsstLog, detrendCallback):
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
        self.lsstLog = lsstLog if lsstLog is not None else {}
        self.detrendCallback = detrendCallback
        self.doGenDetrendKey = detrendCallback.get('activated', False)

        self.pfsVisits = {}  # visitId -> list of exposure ids
        self.rawButler = None  # butler for raw/ingest operations
        self.dotRoach = None
        self.configOverride = None  # no config override yet.

        # Enable auto-ingest and auto-reduction by default
        self.doAutoIngest = True
        self.doAutoReduce = True

        # Initialize Butler instances and handlers
        self.rawButler = self.loadButler(self.rawRun)
        self.pfsConfigButler = self.loadButler(self.pfsConfigRun)
        self.butler = Butler(self.datastore, collections=[self.inputCollection, self.outputCollection])

        self.ingestHandler = IngestHandler(self)
        self.reducePipeline, self.timestamp = self.setupReducePipeline(datastore, outputCollection, pipelineYaml)
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
                   detrendCallback=detrendCallback)

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
        Resolve an IIC sequence into PFS visits, check ingestion state, and optionally run group reduction.

        The method queries opDB for all pfs_visit_id belonging to the given iic_sequence_id, verifies that each visit
        exists in self.pfsVisits and is already ingested, then, if self.groupVisit is True, runs the reduction
        pipeline on the set via processVisitGroup. Visits sharing a single sps_visit.exp_type are reduced with the
        config overrides given by groupConfigOverride, a group mixing exp_types with the pipeline defaults.

        Note that by construction newVisitGroup is called after the end of newVisit, meaning that all related visits
        should already be ingested.

        Parameters
        ----------
        sequenceId : int or str
            iic_sequence.iic_sequence_id to resolve into one or more pfs_visit_id entries. Cast to int internally.
        groupId : int or str
            Group identifier from IIC; currently used for bookkeeping/logging (not interpreted here).
        sequenceType : str
            Type of the IIC sequence (e.g. 'scienceObject', 'arc', 'flat'); see groupConfigOverride.
        name : str
            Human-readable sequence name from IIC.
        comments : str
            Free-form comment string associated with the sequence.
        cmdStr : str
            Original IIC command string that created the sequence.
        status : str
            Sequence status as reported by IIC (e.g. 'DONE', 'FAILED'); currently not interpreted here.
        output : dict or Any
            Additional output payload from IIC; currently passed through unchanged.
        """
        try:
            sequenceId = int(sequenceId)
        except Exception:
            self.logger.warning(f'Invalid sequenceId {sequenceId!r}')
            return

        self.logger.info(f'New iic_sequence sequenceId={sequenceId} groupId={groupId} '
                         f'sequenceType={sequenceType} name={name!r} comments={comments!r}')

        sql = (
            'SELECT sps_visit.pfs_visit_id, sps_visit.exp_type '
            'FROM visit_set INNER JOIN iic_sequence ON visit_set.iic_sequence_id = iic_sequence.iic_sequence_id '
            'INNER JOIN sps_visit ON visit_set.pfs_visit_id=sps_visit.pfs_visit_id '
            f'WHERE visit_set.iic_sequence_id = {sequenceId}'
        )
        visitRows = opDB.fetchall(sql)  # numpy array of str, like [['123', 'arc'], ['124', 'arc']]

        if not len(visitRows):
            self.logger.warning(f'Could not match any pfs_visit_id for iic_sequence_id {sequenceId}')
            return

        expTypes = {int(vid): str(expType) for vid, expType in visitRows}

        missing = []
        notIngested = []

        for vid in expTypes:
            if vid in self.pfsVisits:
                if not self.pfsVisits[vid].isIngested:
                    notIngested.append(vid)
            else:
                missing.append(vid)

        if missing:
            self.logger.warning(
                f'visitGroup ({sequenceId}) : Could not match all related visits in PfsVisit dictionary; missing {missing}')
            return

        if notIngested:
            self.logger.warning(f'visitGroup ({sequenceId}) : Following PfsVisit are not ingested {notIngested}')
            return

        if self.groupVisit:
            uniqueExpTypes = sorted(set(expTypes.values()))

            if len(uniqueExpTypes) == 1:
                self.addConfigOverride(groupConfigOverride(uniqueExpTypes[0], str(sequenceType)))
            else:
                self.logger.warning(f'visitGroup ({sequenceId}) : mixed exp_type {uniqueExpTypes}, '
                                    f'reducing without config override')
                # an empty override marks the run as holding the pipeline defaults, so the next override rolls a new run.
                if self.configOverride:
                    self.newReduceRun(configOverride={})
                else:
                    self.addConfigOverride({})

            self.processVisitGroup([self.pfsVisits[vid] for vid in expTypes])

    def processPfsVisit(self, pfsVisit):
        """
        Ingest and reduce data for a single PFS visit, including optional callbacks and tools.

        Parameters
        ----------
        pfsVisit : PfsVisit
            The visit object containing exposures and configurations.
        """
        if self.doAutoIngest:
            self.ingestHandler.doIngest(pfsVisit)

        if pfsVisit.isIngested and not self.groupVisit:
            if self.doAutoReduce:
                # setting up a callback to be generated when the file is generated.
                if self.doGenDetrendKey:
                    pfsVisit.setupDetrendCallback(self)

                # Running the pipeline for that visit.
                self.runReductionPipeline(where=f"visit={pfsVisit.visit}")

            # run roaches ! run !
            if self.dotRoach is not None:
                self.dotRoach.run(pfsVisit)

        # Just declaring that visit should be processed, just a placeholder for now.
        pfsVisit.finish()

    def processVisitGroup(self, pfsVisits):
        """
        Reduce a group of already ingested PFS visits in one pipeline execution and finalize them.

        The input visits are assumed to exist in self.pfsVisits and be marked ingested (validated by newVisitGroup).
        The method logs the visit IDs, runs the reduction pipeline once with a "visit in (...)" WHERE clause, then
        calls finish() on each PfsVisit regardless of pipeline outcome.

        Parameters
        ----------
        pfsVisits : list[PfsVisit]
            Visits to process together.

        Notes
        -----
        - Process concurrency is controlled by self.numProc; per-quantum threading by self.taskThreads.
        - The pipeline outcome is reported in reduceExposureStatus; all visits are finished regardless.
        """
        visitStr = ','.join(str(p.visit) for p in pfsVisits)

        self.logger.info(f'processVisitGroup started on {visitStr}')
        t0 = time.perf_counter()
        returnCode = self.runReductionPipeline(where=f"visit in ({visitStr})")
        t1 = time.perf_counter()

        cmd = self.actor.bcast
        statusStr = 'OK' if returnCode == 0 else 'FAILED'

        for p in pfsVisits:
            p.finish()
            cmd.inform(f'reduceExposureStatus={p.visit},{returnCode},"{statusStr}",{t1 - t0:.1f}')

    def runReductionPipeline(self, where, retry=True):
        """
        Run the reduction pipeline on the selected data, in a pipetask subprocess extending the current run.

        Parameters
        ----------
        where : str
            Data query selecting what to reduce.
        retry : bool
            Allow a single retry in a new run when the current run holds other software versions or task
            configs.

        Returns
        -------
        returnCode : int
            pipetask exit code, 0 on success, -1 if pipetask could not be run.
        """
        run = os.path.join(self.outputCollection, self.timestamp)
        # the pipeline only knows the overrides of its own tasks.
        configOverride = {label: cfg for label, cfg in (self.configOverride or {}).items()
                          if label in self.reducePipeline.task_labels}

        cmd = pipetaskCommand(self.datastore, self.inputCollection, run, self.pipelinePath(self.pipelineYaml),
                              where, configOverride=configOverride, numProc=self.numProc,
                              taskThreads=self.taskThreads, failFast=self.fail_fast,
                              clobberOutput=self.clobberOutput)

        logDir = os.path.expandvars(self.actor.actorConfig['logging']['logdir'])
        now = datetime.datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        logFile = os.path.join(logDir, 'pipetask', self.timestamp, f'{now}.log')

        self.logger.info(f'pipetask where="{where}" num_proc={self.numProc} fail_fast={self.fail_fast} '
                         f'log={logFile}')
        try:
            returnCode = runPipetask(cmd, logFile)
        except Exception as e:
            self.logger.exception(e)
            return -1

        self.logger.info(f'pipetask where="{where}" returned {returnCode}')

        if returnCode != 0 and retry and runIsInconsistent(logFile):
            # A run holds a single packages dataset and a single config per task, so a conda package updated
            # underneath the actor makes that run unusable, only a new one can record the new versions.
            self.logger.warning('Run holds other software versions or task configs, retrying in a new run.')
            self.newReduceRun(configOverride=self.configOverride)
            return self.runReductionPipeline(where, retry=False)

        return returnCode

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

    def mergedConfigOverride(self, configOverride):
        """
        The active config overrides updated with new ones, leaving the active ones untouched.

        Parameters
        ----------
        configOverride : dict
            {taskLabel: {configKey: value}}.

        Returns
        -------
        merged : dict
            {taskLabel: {configKey: value}}.
        """
        merged = {label: dict(cfg) for label, cfg in (self.configOverride or {}).items()}

        for label, cfg in configOverride.items():
            merged.setdefault(label, {}).update(cfg)

        return merged

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
        merged = self.mergedConfigOverride(configOverride)

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
