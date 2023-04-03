import concurrent.futures
import time

from drpActor.utils.tasks.detrend import DetrendTask
from drpActor.utils.tasks.ipc import IPCTask
from twisted.internet import reactor


class TasksExec:

    def __init__(self, engine):
        self.engine = engine

        self.defects = dict()
        self.executor = concurrent.futures.ProcessPoolExecutor(engine.nProcesses)

        self.ingestTask = engine.ingestFlavour.bootstrap(self)
        self.detrendTask = DetrendTask.bootstrap(self)
        self.ipcTask = IPCTask.bootstrap(self)

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

    def detrend(self, file, **options):
        """"""
        file.state = 'processing'
        if file.arm == 'n':
            # always get defects from the main process.
            self.getDefects(file.dataId)
            self.ipcTask.runFromActor(file)
        else:
            self.detrendTask.runFromActor(file, **options)

    def detrendDoneCB(self, file, *args, sleepTime=0.1, timeout=10, **kwargs):
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

