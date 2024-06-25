import concurrent.futures
import logging
import time
import traceback

import drpActor.utils.tasks.ingest as ingest
from drpActor.utils.tasks.detectorMapQa import DetectorMapQaTask
from drpActor.utils.tasks.detrend import DetrendTask
from drpActor.utils.tasks.extractionQa import ExtractionQaTask
from drpActor.utils.tasks.ipc import IPCTask
from drpActor.utils.tasks.reduceExposure import ReduceExposureTask
from twisted.internet import reactor


class TasksExec:
    cacheCalibs = ['defects', 'flat', 'ipc']

    def __init__(self, engine):
        self.engine = engine

        self.calibs = dict()
        self.executor = concurrent.futures.ProcessPoolExecutor(engine.nProcesses)

        self.ingestTask = ingest.factory(engine.settings['ingestPgsql']).bootstrap(self)
        self.detrendTask = DetrendTask.bootstrap(self)
        self.ipcTask = IPCTask.bootstrap(self)
        self.reduceExposureTask = ReduceExposureTask.bootstrap(self)
        self.detectorMapQaTask = DetectorMapQaTask.bootstrap(self)
        self.extractionQaTask = ExtractionQaTask.bootstrap(self)

        self.actor.cleanLogHandlers()  # the drp parsers add some unwanted handlers, get rid of those.

    @property
    def actor(self):
        return self.engine.actor

    @property
    def butler(self):
        return self.engine.butler

    def getCalibs(self, dataId):
        """getting calibs from butler or memory."""
        cameraKey = dataId['spectrograph'], dataId['arm']

        if cameraKey not in self.calibs:
            calib = dict()

            for calibName in TasksExec.cacheCalibs:
                self.actor.logger.info(f'loading {calibName} for {cameraKey}')
                calib[calibName] = self.butler.get(calibName, dataId)

            self.calibs[cameraKey] = calib

        return self.calibs[cameraKey]

    def ingest(self, file):
        """"""
        self.ingestTask.runFromActor(file)
        file.getRawMd(self.butler)

    def detrend(self, file):
        """"""
        file.state = 'processing'
        if file.arm == 'n':
            # always get defects from the main process.
            try:
                self.getCalibs(file.dataId)
            except Exception:
                logging.warning('An error occurred while running ISR on dataId %s:\n%s', file.dataId,
                                traceback.format_exc())
            self.ipcTask.runFromActor(file)
        else:
            self.detrendTask.runFromActor(file)

    def reduceExposure(self, file):
        """"""
        file.state = 'processing'
        self.reduceExposureTask.runFromActor(file)

    def doDetectorMapQa(self, file):
        file.state = 'QA'
        self.detectorMapQaTask.runFromActor(file)

    def doExtractionQa(self, file):
        file.state = 'QA'
        self.extractionQaTask.runFromActor(file)

    def waitForCalexp(self, file, *args, sleepTime=0.1, timeout=10, **kwargs):
        """CallBack called whenever an H4 image is detrended."""

        def fromThisThread():
            start = time.time()

            while not file.getCalexp(self.butler):
                if (time.time() - start) > timeout:
                    file.state = 'idle'
                    self.engine.logger.warning(f'failed to get calexp for {str(file.dataId)}')
                    return

                time.sleep(sleepTime)

            file.state = 'idle'
            self.engine.genDetrendKey(file)

        reactor.callLater(0.1, fromThisThread)

    def waitForPfsArm(self, file, *args, sleepTime=0.1, timeout=10, **kwargs):
        """CallBack called whenever an H4 image is detrended."""

        def fromThisThread():
            start = time.time()

            while not file.getPfsArm(self.butler):
                if (time.time() - start) > timeout:
                    file.state = 'idle'
                    self.engine.logger.warning(f'failed to get pfsArm for {str(file.dataId)}')
                    return

                time.sleep(sleepTime)

            file.state = 'idle'
            self.engine.genPfsArmKey(file)

        reactor.callLater(0.1, fromThisThread)

    def waitForDetectorMapQa(self, file, *args, sleepTime=0.1, timeout=10, **kwargs):
        """CallBack called whenever an H4 image is detrended."""

        def fromThisThread():
            start = time.time()

            while not file.getDmQaResidualImage(self.butler):
                if (time.time() - start) > timeout:
                    file.state = 'idle'
                    self.engine.logger.warning(f'failed to get detectorMapQA for {str(file.dataId)}')
                    return

                time.sleep(sleepTime)

            file.state = 'idle'
            self.engine.genDetectorMapQaKey(file)

        reactor.callLater(0.1, fromThisThread)

    def waitForExtractionQa(self, file, *args, sleepTime=0.1, timeout=10, **kwargs):
        """CallBack called whenever an H4 image is detrended."""

        def fromThisThread():
            start = time.time()

            while not file.getExtQaStats(self.butler):
                if (time.time() - start) > timeout:
                    file.state = 'idle'
                    self.engine.logger.warning(f'failed to get extractionQa for {str(file.dataId)}')
                    return

                time.sleep(sleepTime)

            file.state = 'idle'
            self.engine.genExtractionQaKey(file)

        reactor.callLater(0.1, fromThisThread)
