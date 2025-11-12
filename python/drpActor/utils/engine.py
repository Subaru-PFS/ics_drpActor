import datetime
import logging
import os
from datetime import timezone
from importlib import reload

import drpActor.utils.dotRoach as dotRoach

reload(dotRoach)

from lsst.daf.butler import Butler
from drpActor.utils.pfsVisit import PfsVisit
from drpActor.utils.lsstLog import setLsstLongLog
from drpActor.utils.tasks.ingest import IngestHandler
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.pipe.base import Pipeline, ExecutionResources
from drpActor.utils.chainedCollection import extend_collection_chain
from ics.utils.opdb import opDB


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
    lsstLog : dict
        LSST logging configuration (e.g., {"level": 10, "long_log": False}).
    detrendCallback : dict
        Detrend-key callback configuration (e.g., {"activated": False, "useSilentThread": True,
        "waitInterval": 1, "timeout": 90}).

    Notes
    -----
    - `numProc` controls **process** parallelism at the executor level.
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
        self.groupVisit = groupVisit  # reduce visits as a group.
        self.fail_fast = fail_fast  # run pipeline in fail_fast mode.

        # execution/logging/callback options
        self.numProc = numProc  # number of worker processes (process-level parallelism)
        self.taskThreads = taskThreads
        self.clobberOutput = clobberOutput
        self.lsstLog = lsstLog
        self.detrendCallback = detrendCallback
        self.doGenDetrendKey = detrendCallback.get('activated', False)

        self.pfsVisits = {}  # visitId -> list of exposure ids
        self.rawButler = None  # butler for raw/ingest operations
        self.dotRoach = None

        # Enable auto-ingest and auto-reduction by default
        self.doAutoIngest = True
        self.doAutoReduce = True

        # Initialize Butler instances and handlers
        self.rawButler = self.loadButler(self.rawRun)
        self.pfsConfigButler = self.loadButler(self.pfsConfigRun)
        self.butler = Butler(self.datastore, collections=[self.inputCollection, self.outputCollection])

        self.ingestHandler = IngestHandler(self)
        self.reducePipeline, self.reduceButler, self.executor, self.timestamp = self.setupReducePipeline(datastore,
                                                                                                         inputCollection,
                                                                                                         outputCollection,
                                                                                                         pipelineYaml,
                                                                                                         taskThreads)
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

    def setupReducePipeline(self, datastore, inputCollection, chainedCollection, pipelineYaml, taskThreads=1):
        """
        Set up the reduction pipeline and its executor.

        Parameters
        ----------
        datastore : str
            Path to the datastore (Butler repo root/URI).
        inputCollection : str
            Name of the input collection.
        chainedCollection : str
            Name of the output chained collection (base of the run).
        pipelineYaml : str
            Path (relative to $PFS_INSTDATA_DIR/config) to the pipeline YAML.
        taskThreads : int, optional
            Threads per quantum (ExecutionResources.num_cores). Does not control process-level parallelism.

        Returns
        -------
        tuple
            (pipeline, butler, executor, timestamp)

        Notes
        -----
        - Use `self.numProc` later with `run_pipeline(..., num_proc=self.numProc)` to control process concurrency.
        - `taskThreads` only affects intra-quantum threading; keep 1 when BLAS/OpenMP are pinned to 1.
        """
        # Load the reduction pipeline from YAML
        pipeline = Pipeline.fromFile(os.path.join(os.getenv("PFS_INSTDATA_DIR"), 'config', pipelineYaml))

        # Append a timestamp to the output collection name
        timestamp = datetime.datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        run = os.path.join(chainedCollection, timestamp)

        # Initialize the Butler with the input and output collections
        butler = Butler(datastore, collections=[inputCollection], run=run)

        # extend collection chaine
        extend_collection_chain(datastore, chainedCollection, run, logger=self.logger)

        # Set up the pipeline executor for parallel processing
        executor = SeparablePipelineExecutor(butler=butler, clobber_output=True,
                                             resources=ExecutionResources(num_cores=taskThreads))

        return pipeline, butler, executor, timestamp

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

    def newVisitGroup(self, sequenceId):
        """
        Resolve an IIC sequence into PFS visits and process them as a group.

        The method queries opDB for all pfs_visit_id belonging to the given iic_sequence_id, verifies that each visit
        exists in self.pfsVisits and is already ingested, then runs the reduction pipeline on the set. If any visit is
        missing or not ingested, it logs a warning and returns without running the group reduction.

        Note that by construction newVisitGroup is called after the end of newVisit, meaning that all related visits
        should already be ingested.

        Parameters
        ----------
        sequenceId : int
            iic_sequence.iic_sequence_id to resolve into one or more pfs_visit_id entries.
        """
        try:
            sequenceId = int(sequenceId)
        except Exception:
            self.logger.warning(f'Invalid sequenceId {sequenceId!r}')
            return

        sql = (
            'SELECT sps_visit.pfs_visit_id '
            'FROM visit_set INNER JOIN iic_sequence ON visit_set.iic_sequence_id = iic_sequence.iic_sequence_id '
            'INNER JOIN sps_visit ON visit_set.pfs_visit_id=sps_visit.pfs_visit_id '
            f'WHERE visit_set.iic_sequence_id = {sequenceId}'
        )
        visitRows = opDB.fetchall(sql)  # expected like [(123,), (124,)] or [123, 124]

        if not visitRows.any():
            self.logger.warning(f'Could not match any pfs_visit_id for iic_sequence_id {sequenceId}')
            return

        # Normalize to a flat list of visit IDs
        visitIds = [row[0] for row in visitRows]

        pfsVisits = []
        missing = []
        notIngested = []

        for vid in visitIds:
            if vid in self.pfsVisits:
                if self.pfsVisits[vid].isIngested:
                    pfsVisits.append(self.pfsVisits[vid])
                else:
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

        self.processVisitGroup(pfsVisits)

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
        - Any pipeline exception is logged upstream; this method still attempts to finish all visits.
        """
        # not processing pfsVisits obviously.
        if not self.groupVisit:
            return

        visitStr = ','.join(str(p.visit) for p in pfsVisits)

        self.logger.info(f'processVisitGroup started on {visitStr}')
        self.runReductionPipeline(where=f"visit in ({visitStr})")

        for p in pfsVisits:
            p.finish()

    def runReductionPipeline(self, where):
        """
        Execute the reduction pipeline for a given visit.

        Parameters
        ----------
        where : str
            Query to filter the data for the specific visit.
        """
        try:
            quantumGraph = self.executor.make_quantum_graph(pipeline=self.reducePipeline, where=where)
        except Exception as e:
            self.logger.exception(e)
            return

        self.executor.pre_execute_qgraph(quantumGraph)

        # setting long_log disgustingly.
        long_log = self.lsstLog.get('long_log', False)
        level = self.lsstLog.get('level', logging.INFO)
        if long_log:
            setLsstLongLog(level)

        self.logger.info(f'run_pipeline where="{where}" num_proc={self.numProc} fail_fast={self.fail_fast}')
        # passing down num_proc for the most recent version.
        self.executor.run_pipeline(graph=quantumGraph, num_proc=self.numProc, fail_fast=self.fail_fast)

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
