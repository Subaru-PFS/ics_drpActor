import concurrent.futures
import logging
import os
import shutil
from importlib import reload
from functools import partial
import drpActor.utils.cmdList as cmdList
import drpActor.utils.tasks.tasksBox as tasksBox
import drpActor.utils.tasks.ingest as ingestTask
import drpActor.utils.dotRoach as dotRoach
import lsst.daf.persistence as dafPersist

reload(cmdList)
reload(dotRoach)
from twisted.internet import reactor


def doIPCTask(dataId):
    """Doing IPC task, note that this is called from multiprocessing, there isn't access to the actual DrpEngine
    instance."""
    # raw image is actually :  hdu[-1] - hdu[0], both reference pixel subtracted, reset frame is ignored.
    cds = drpEngine.butler.get('raw', dataId=dataId)
    defects = drpEngine.getDefects(dataId)

    logging.info(f'running doIPCTask for {dataId}')
    calexp = drpEngine.tasks.ipcTask.run(cds, defects=defects).exposure

    logging.info(f'doIPCTask done, putting output to butler.')
    drpEngine.butler.put(calexp, 'calexp', dataId)


class DrpEngine(object):
    def __init__(self, actor, target, CALIB, rerun, pfsConfigDir, nProcesses, **config):
        self.actor = actor
        self.target = target
        self.CALIB = CALIB
        self.rerun = rerun
        self.pfsConfigDir = pfsConfigDir
        self.nProcesses = nProcesses

        self.config = config

        self.fileBuffer = []
        self.dotRoach = None
        self.defects = dict()

        # default settings
        self.doCopyDesignToPfsConfigDir = False
        self.doAutoIngest = True
        self.ingestMode = 'link'
        self.ingestFlavour = ingestTask.IngestTask
        self.doAutoDetrend = True
        self.doAutoReduce = False

        self.executor = concurrent.futures.ProcessPoolExecutor(nProcesses)
        self.tasks = tasksBox.TasksBox(self)

        # for hilo base only.
        if self.actor.site == 'H':
            self.ingestFlavour = ingestTask.PgsqlIngestTask
            self.ingestMode = 'copy'
            self.doAutoDetrend = False
            self.doAutoReduce = True

        self.butler = self.loadButler()

        # declaring global drpEngine for multiprocessing.
        global drpEngine
        drpEngine = self

    @classmethod
    def fromConfigFile(cls, actor):
        return cls(actor, **actor.actorConfig[actor.site])

    def addFile(self, file):
        """ Add new file into the buffer. """
        # Assessing what has been done on this file/dataId .
        calexp = file.getCalexp(self.butler)
        pfsArm = file.getPfsArm(self.butler)

        self.actor.logger.info(f'newFile: {str(file.dataId)} state={file.current} calexp={file.calexp} pfsArm={file.pfsArm}')

        self.fileBuffer.append(file)

    def loadButler(self):
        """load butler. """
        try:
            butler = dafPersist.Butler(os.path.join(self.target, 'rerun', self.rerun))
        except Exception as e:
            self.actor.logger.warning('text=%s' % self.actor.strTraceback(e))
            self.actor.logger.warning('butler could not be instantiated, might be a fresh one')
            butler = None

        return butler

    def lookupMetaData(self, file):
        """ lookup into metadata, search here if exposure is windowed."""

        def isWindowed(md):
            try:
                nRows = md['W_CDROWN'] + 1 - md['W_CDROW0']
                return nRows != 4300
            except KeyError:
                return False

        md = self.butler.get('raw_md', **file.dataId)
        return dict(windowed=isWindowed(md))

    def isrRemoval(self, visit, genKeys=True):
        """Proceed with isr removal for that visit."""

        def cacheDefects(ingested):
            """Load NIR arm defect."""
            nirFiles = [file for file in ingested if file.arm == 'n']

            for file in nirFiles:
                self.getDefects(file.dataId)

            drpEngine.defects = self.defects

        def doIngest(files):
            """"""
            toIngest = [file for file in files if file.unknown]

            if toIngest:
                self.tasks.ingest(toIngest)

                # just setting state-machine
                for file in toIngest:
                    file.ingest()

                if genKeys:
                    self.actor.bcast.inform(f'ingestStatus={visit},{0},{"OK"},{0}')

        def detrendDoneCB(file, *args, **kwargs):
            """CallBack called whenever an H4 image is detrended."""
            reactor.callLater(2, self.genDetrendKey, file)

        # get exposure with same visit
        sameVisit = [file for file in self.fileBuffer if file.visit == visit]
        if not sameVisit:
            return

        if self.doAutoIngest:
            doIngest(sameVisit)

        ingested = [file for file in sameVisit if file.isIngested(self.butler)]

        # loading defects first, should be done only once.
        cacheDefects(ingested)

        if self.doAutoDetrend:
            toDetrend = []
            for file in ingested:
                # calexp file already exists.
                if file.calexp:
                    self.genDetrendKey(file)
                    continue

                if file.reducible:
                    toDetrend.append(file)

            nirFiles = [file for file in toDetrend if file.arm == 'n']
            ccdFiles = [file for file in toDetrend if file.arm in 'brm']

            if nirFiles:
                for file in nirFiles:
                    # just setting state-machine
                    file.reduce()
                    # calling multiprocessing to do IPCTask.
                    future = self.executor.submit(doIPCTask, file.dataId)
                    future.add_done_callback(partial(detrendDoneCB, file))

            if ccdFiles:
                options = self.lookupMetaData(ccdFiles[0])

                # just setting state-machine
                for file in ccdFiles:
                    file.reduce()

                self.tasks.detrend(visit, len(ccdFiles), **options)

                # cmd = cmdList.Detrend(self, visit, len(ccdFiles), **options)
                # status, statusStr, timing = cmd.run()
                # self.actor.bcast.inform(f'detrendStatus={visit},{status},{statusStr},{timing}')

                for file in ccdFiles:
                    self.genDetrendKey(file)

        #
        # if self.doAutoReduce:
        #     if ccdFiles:
        #         options = self.lookupMetaData(files[0])
        #         cmd = cmdList.ReduceExposure(self.target, self.CALIB, self.rerun, visit, **options)
        #         genCommandStatus('reduceExposure', *cmd.run())

        if self.dotRoach is not None:
            self.dotRoach.runAway(sameVisit)
            self.dotRoach.status(cmd=self.actor.bcast)

    def genDetrendKey(self, file):
        """"""
        file.idle()
        if file.getCalexp(self.butler):
            self.actor.bcast.inform(f"detrend={file.calexp}")
            self.fileBuffer.remove(file)
        else:
            self.actor.logger.warning(f'failed to get calexp for {str(file.dataId)}')

    def newPfsDesign(self, designId):
        """New PfsDesign has been declared, copy it to the repo if new."""
        if not self.doCopyDesignToPfsConfigDir:
            return

        fileName = f'pfsDesign-0x{designId:016x}.fits'

        if not os.path.isfile(os.path.join(self.pfsConfigDir, fileName)):
            designPath = os.path.join('/data/pfsDesign', fileName)
            try:
                shutil.copy(designPath, self.pfsConfigDir)
                self.actor.logger.info(f'{fileName} copied {self.pfsConfigDir} successfully !')
            except Exception as e:
                self.actor.logger.warning(f'failed to copy from {designPath} to {self.pfsConfigDir} with {e}')

    def getDefects(self, dataId):
        """getting defects from butler or memory."""
        cameraKey = dataId['spectrograph'], dataId['arm']

        if cameraKey not in self.defects:
            self.actor.logger.info(f'loading defects for {cameraKey}')
            defects = self.butler.get("defects", dataId)

            self.defects[cameraKey] = defects

        return self.defects[cameraKey]

    def startDotRoach(self, dataRoot, maskFile, keepMoving=False):
        """ Starting dotRoach loop, deactivating autodetrend. """
        self.doAutoDetrend = False
        self.doAutoReduce = False
        self.dotRoach = dotRoach.DotRoach(self, dataRoot, maskFile, keepMoving=keepMoving)

    def stopDotRoach(self, cmd):
        """ stopping dotRoach loop, reactivating autodetrend. """
        self.doAutoIngest = True
        self.doAutoDetrend = True
        self.doAutoReduce = False

        if not self.dotRoach:
            cmd.warn('text="no dotRoach loop on-going."')
            return

        cmd.inform('text="ending dotRoach loop"')
        self.dotRoach.finish()
        self.dotRoach = None

        self.actor.callCommand('checkLeftOvers')

    def checkLeftOvers(self, cmd):
        """ """
        for visit in list(set([file.visit for file in self.fileBuffer])):
            cmd.inform(f'text="found visit:{visit} in leftovers"')
            # self.isrRemoval(visit)
