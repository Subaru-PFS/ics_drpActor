import datetime
import logging
import os
from importlib import reload

import drpActor.utils.dotRoach as dotRoach

reload(dotRoach)

from lsst.daf.butler import Butler
from drpActor.utils.pfsVisit import PfsVisit
from drpActor.utils.lsstLog import setLsstLongLog
from drpActor.utils.tasks.ingest import IngestHandler
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.pipe.base import Pipeline
from drpActor.utils.chainedCollection import extend_collection_chain
import multiprocessing as mp


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
        Identifier for the raw data run.
    pfsConfigRun : str
        Identifier for the PFS configuration run.
    ingestMode : str
        Mode for ingestion (e.g., automatic or manual).
    inputCollection : str
        Name of the input collection used for reduction.
    outputCollection : str
        Name of the output collection where results will be stored.
    pipelineYaml : str
        Path to the YAML configuration file for the reduction pipeline.
    maxWorkers : int
        Maximum number of subprocesses (quanta) to run in parallel.
    **config : dict
        Additional configuration parameters for the engine.
    """

    def __init__(self, actor, datastore, rawRun, pfsConfigRun, ingestMode,
                 inputCollection, outputCollection, pipelineYaml, maxWorkers, doGenDetrendKey=False, **config):
        """Initialize the DrpEngine with actor, datastore, collections, and settings."""
        self.actor = actor  # Reference to the actor for logging and configuration
        self.datastore = datastore  # Path to the data storage location
        self.rawRun = rawRun  # Run ID for raw data
        self.pfsConfigRun = pfsConfigRun  # Run ID for PFS configuration data
        self.ingestMode = ingestMode  # Ingestion mode (automatic or manual)
        self.inputCollection = inputCollection  # Name of the input collection
        self.outputCollection = outputCollection  # Name of the output collection
        self.maxWorkers = maxWorkers  # Max number of subprocesses to run in parallel
        self.doGenDetrendKey = doGenDetrendKey
        self.config = config  # Additional configuration parameters
        self.pfsVisits = {}  # Dictionary to store visits and their exposures
        self.rawButler = None  # Butler instance for raw data handling

        self.dotRoach = None

        # Enable auto-ingest and auto-reduction by default
        self.doAutoIngest = True
        self.doAutoReduce = True

        # Initialize Butler instances and handlers
        self.rawButler = self.loadButler(self.rawRun)
        self.pfsConfigButler = self.loadButler(self.pfsConfigRun)
        # just convenient for now.
        self.butler = Butler(self.datastore, collections=[self.inputCollection, self.outputCollection])

        self.ingestHandler = IngestHandler(self)
        self.reducePipeline, self.reduceButler, self.executor, self.timestamp = self.setupReducePipeline(datastore,
                                                                                                         inputCollection,
                                                                                                         outputCollection,
                                                                                                         pipelineYaml,
                                                                                                         maxWorkers)
        self.condaEnv = os.environ.get("CONDA_DEFAULT_ENV")

    @property
    def logger(self):
        """Retrieve the logger instance from the actor."""
        return self.actor.logger

    @classmethod
    def fromConfigFile(cls, actor):
        """
        Create a DrpEngine instance from the actor's configuration file.

        Parameters
        ----------
        actor : object
            The actor instance with access to the configuration file.

        Returns
        -------
        DrpEngine
            A configured instance of the DrpEngine class.
        """
        return cls(actor, **actor.actorConfig[actor.site])

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

    def setupReducePipeline(self, datastore, inputCollection, chainedCollection, pipelineYaml, maxWorkers):
        """
        Set up the reduction pipeline and its executor.

        Parameters
        ----------
        datastore : str
            Path to the datastore.
        inputCollection : str
            Name of the input collection.
        chainedCollection : str
            Name of the output chained collection.
        pipelineYaml : str
            Path to the YAML file defining the reduction pipeline.
        maxWorkers : int
            Number of cores for parallel processing.

        Returns
        -------
        tuple
            A tuple containing the pipeline, Butler instance, and executor.
        """
        # Load the reduction pipeline from YAML
        pipeline = Pipeline.fromFile(os.path.join(os.getenv("PFS_INSTDATA_DIR"), 'config', pipelineYaml))

        # Append a timestamp to the output collection name
        timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        run = os.path.join(chainedCollection, timestamp)

        # Initialize the Butler with the input and output collections
        butler = Butler(datastore, collections=[inputCollection], run=run)

        # extend collection chaine
        extend_collection_chain(datastore, chainedCollection, run, logger=self.logger)

        # Set up the pipeline executor for parallel processing
        executor = SeparablePipelineExecutor(butler=butler, clobber_output=True)

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

    def processPfsVisit(self, pfsVisit):
        """
        Ingest and reduce data for a PFS visit.

        Parameters
        ----------
        pfsVisit : PfsVisit
            The visit object containing exposures and configurations.
        """
        if self.doAutoIngest:
            self.ingestHandler.doIngest(pfsVisit)

        if pfsVisit.isIngested:
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
        long_log = self.actor.actorConfig['lsstLog'].get('long_log', False)
        level = self.actor.actorConfig['lsstLog'].get('level', logging.INFO)
        if long_log:
            setLsstLongLog(level)

        self.logger.info(f"multiprocessing start method: {mp.get_start_method()}")

        logging.getLogger('lsst.ctrl.mpexec.mpGraphExecutor').setLevel(logging.DEBUG)

        # passing down num_proc for the most recent version.
        self.executor.run_pipeline(graph=quantumGraph, num_proc=self.maxWorkers)

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
