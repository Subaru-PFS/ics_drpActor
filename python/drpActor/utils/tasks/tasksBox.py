from drpActor.utils.tasks.detrend import DetrendTask
from drpActor.utils.tasks.ipcTask import IPCTask
import concurrent.futures
from twisted.internet import reactor
from functools import partial
import logging

class TasksBox:

    def __init__(self, engine):
        self.engine = engine

        self.defects = dict()
        
        self.executor = concurrent.futures.ProcessPoolExecutor(engine.nProcesses)

        self.ingestTask = engine.ingestFlavour.bootstrap(self)
        self.detrendTask = DetrendTask.bootstrap(self)
        self.ipcTask = IPCTask.bootstrap()
        
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

    def ingest(self, files):
        filepath = [file.filepath for file in files]
        self.ingestTask.runFromActor(filepath)

    def detrend(self, file, **options):
        self.detrendTask.runFromActor(file, **options)

    def detrendDoneCB(self, file, *args, **kwargs):
        """CallBack called whenever an H4 image is detrended."""
        reactor.callLater(2, self.engine.genDetrendKey, file)

    def ipcTask(self, file):
        def doIPCTask(dataId):
            """Doing IPC task, note that this is called from multiprocessing, there isn't access to the actual tasta
            instance."""
            # raw image is actually :  hdu[-1] - hdu[0], both reference pixel subtracted, reset frame is ignored.
            cds = tasta.butler.get('raw', dataId=dataId)
            defects = tasta.getDefects(dataId)

            logging.info(f'running doIPCTask for {dataId}')
            calexp = tasta.ipcTask.run(cds, defects=defects).exposure

            logging.info(f'doIPCTask done, putting output to butler.')
            tasta.butler.put(calexp, 'calexp', dataId)

        self.getDefects(file.dataId)
        global tasta
        tasta = self

        # just setting state-machine
        file.reduce()
        # calling multiprocessing to do IPCTask.
        future = self.executor.submit(doIPCTask, file.dataId)
        future.add_done_callback(partial(self.detrendDoneCB, file))
