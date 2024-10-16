from importlib import reload

import drpActor.utils.dotRoach as dotRoach

reload(dotRoach)


class DrpEngine(object):
    maxAttempts = 100
    timeBetweenAttempts = 15

    def __init__(self, actor, target, CALIB, rerun, pfsConfigDir, nProcesses, **config):
        self.actor = actor
        self.target = target
        self.CALIB = CALIB
        self.rerun = rerun
        self.pfsConfigDir = pfsConfigDir
        self.nProcesses = nProcesses

        self.config = config

        self.doAutoIngest = False
        self.doAutoDetrend = False
        self.doAutoReduce = False
        self.doDetectorMapQa = False
        self.doExtractionQa = False

        # default setting from config file.
        self.setSettings()

    @property
    def logger(self):
        return self.actor.logger

    @property
    def settings(self):
        return self.config['settings']

    @classmethod
    def fromConfigFile(cls, actor):
        return cls(actor, **actor.actorConfig[actor.site])

    def setSettings(self, doAutoIngest=None, doAutoDetrend=None, doAutoReduce=None,
                    doDetectorMapQa=None, doExtractionQa=None):
        """Setting engine parameters."""
        doAutoIngest = self.settings['doAutoIngest'] if doAutoIngest is None else doAutoIngest
        doAutoDetrend = self.settings['doAutoDetrend'] if doAutoDetrend is None else doAutoDetrend
        doAutoReduce = self.settings['doAutoReduce'] if doAutoReduce is None else doAutoReduce
        doDetectorMapQa = self.settings['doDetectorMapQa'] if doDetectorMapQa is None else doDetectorMapQa
        doExtractionQa = self.settings['doExtractionQa'] if doExtractionQa is None else doExtractionQa

        self.doAutoIngest = doAutoIngest
        self.doAutoDetrend = doAutoDetrend
        self.doAutoReduce = doAutoReduce
        self.doDetectorMapQa = doDetectorMapQa
        self.doExtractionQa = doExtractionQa

    def newExposure(self, file):
        """
        Add new PfsFile into the buffer.

        This function inspects the file, ingests it if it's not already ingested and doAutoIngest is True,
        and then performs detrending and reduction if doAutoDetrend and doAutoReduce are True, respectively.
        If the file has a calexp, a detrend key is generated. If the file has a pfsArm, a pfsArm key is generated.
        All tasks are run in parallel, except for the ingest task which is run in the same thread.

        Parameters:
        file (drpActor.utils.files.PfsFile): The file object to be added into the buffer.

        """
        pass

    def newVisit(self, visit):
        """New sps visit callback."""
        pass
