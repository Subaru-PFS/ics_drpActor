from drpActor.utils.tasks.detrend import DetrendTask
from drpActor.utils.tasks.ipcTask import IPCTask


class TasksBox:

    def __init__(self, engine):
        self.engine = engine

        self.ingestTask = engine.ingestFlavour.bootstrap(self)
        self.detrendTask = DetrendTask.bootstrap(self)
        self.ipcTask = IPCTask.bootstrap()

    def ingest(self, files):
        filepath = [file.filepath for file in files]
        self.ingestTask.runFromActor(filepath)

    def detrend(self, visit, nFiles, **options):
        self.detrendTask.runFromActor(visit, nFiles, **options)
