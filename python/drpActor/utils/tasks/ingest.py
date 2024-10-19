import time

from lsst.obs.base.ingest import RawIngestConfig
from lsst.obs.pfs.gen3 import PfsRawIngestTask
from pfs.drp.stella.gen3 import ingestPfsConfig
import logging

class IngestHandler(object):
    """
    Handles the ingestion of raw data and PFS configurations.

    Parameters
    ----------
    engine : DrpEngine
        Reference to the DRP engine instance.
    """

    def __init__(self, engine):
        self.engine = engine
        self.rawTask = self.createRawTask()

    def createRawTask(self):
        """Initialize the raw ingest task using the configured Butler."""
        if not self.engine.rawButler:
            return None

        config = RawIngestConfig()
        config.transfer = self.engine.ingestMode
        return PfsRawIngestTask(config=config, butler=self.engine.rawButler)

    def ingestPfsConfig(self, pfsVisit):
        """Ingest a pfsConfig file if not already ingested."""
        if pfsVisit.pfsConfigFile.ingested:
            self.engine.logger.warning(f'{pfsVisit.pfsConfigFile.filepath} already ingested.')
            return

        if pfsVisit.pfsConfigFile.filepath is None:
            self.engine.logger.warning(f'no filepath for pfsConfig with visit={pfsVisit.visit}')
            return

        logger = logging.getLogger("pfs.ingestPfsConfig")

        # Clear existing handlers to prevent duplicate log messages
        if logger.hasHandlers():
            logger.handlers.clear()

        pathList = [pfsVisit.pfsConfigFile.filepath]
        ingestPfsConfig(
            self.engine.datastore,
            'PFS',
            self.engine.pfsConfigRun,
            pathList,
            transfer=self.engine.ingestMode,
            update=True
        )

        pfsVisit.pfsConfigFile.initialize(self.engine.pfsConfigButler)

    def doIngest(self, pfsVisit):
        """Ingest both exposure files and the pfsConfig file for a visit."""
        startTime = time.time()
        self.ingestExposureFiles(pfsVisit)
        self.ingestPfsConfig(pfsVisit)

        returnCode = 0
        timing = time.time() - startTime

        if not pfsVisit.isIngested:
            self.engine.actor.bcast.warn(f'ingestStatus={pfsVisit.visit},{returnCode},FAILED,0')
        else:
            self.engine.actor.bcast.inform(f'ingestStatus={pfsVisit.visit},{returnCode},OK,{timing:.1f}')

    def ingestExposureFiles(self, pfsVisit):
        """Ingest all exposure files for the given visit."""
        if not pfsVisit.exposureFiles:
            self.engine.logger.warning(f'No exposure files found for visit {pfsVisit.visit}.')
            return

        toIngest = [file for file in pfsVisit.exposureFiles if not file.ingested]

        if not toIngest:
            self.engine.logger.warning(f'Exposure files already ingested for visit {pfsVisit.visit}.')
            return

        pathList = [file.filepath for file in pfsVisit.exposureFiles]
        self.engine.logger.info(f'Ingesting exposure files for visit {pfsVisit.visit}.')
        self.rawTask.run(pathList)

        for file in toIngest:
            file.initialize(self.engine.rawButler)
