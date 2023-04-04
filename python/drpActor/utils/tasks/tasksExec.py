import concurrent.futures
import time

import drpActor.utils.tasks.ingest as ingest
from drpActor.utils.tasks.detrend import DetrendTask
from drpActor.utils.tasks.ipc import IPCTask
from drpActor.utils.tasks.reduceExposure import ReduceExposureTask
from twisted.internet import reactor


class TasksExec:

    def __init__(self, engine):
        self.engine = engine

        self.defects = dict()
        self.executor = concurrent.futures.ProcessPoolExecutor(engine.nProcesses)

        self.ingestTask = ingest.factory(engine.settings['ingestPgsql']).bootstrap(self)
        self.detrendTask = DetrendTask.bootstrap(self)
        self.ipcTask = IPCTask.bootstrap(self)
        self.reduceExposureTask = ReduceExposureTask.bootstrap(self)

    @property
    def actor(self):
        return self.engine.actor

    @property
    def butler(self):
        return self.engine.butler

    def getDefects(self, dataId):
        """getting defects from butler or memory."""
        cameraKey = dataId['spectrograph'], dataId['arm']

        if cameraKey not in self.defects:
            self.actor.logger.info(f'loading defects for {cameraKey}')
            defects = self.butler.get("defects", dataId)

            self.defects[cameraKey] = defects

        return self.defects[cameraKey]

    def ingest(self, file):
        """"""
        self.ingestTask.runFromActor(file)
        file.getRawMd(self.butler)

    def detrend(self, file):
        """"""
        file.state = 'processing'
        if file.arm == 'n':
            # always get defects from the main process.
            self.getDefects(file.dataId)
            self.ipcTask.runFromActor(file)
        else:
            self.detrendTask.runFromActor(file)

    def reduceExposure(self, file):
        """"""
        file.state = 'processing'
        self.reduceExposureTask.runFromActor(file)

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
            self.engine.logger.info(f'pfsArm successfully generated for {str(file.dataId)} !')

        reactor.callLater(0.1, fromThisThread)
