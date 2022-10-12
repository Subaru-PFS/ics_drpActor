import logging
import os
import shutil
from importlib import reload

import drpActor.utils.cmdList as cmdList
import drpActor.utils.dotRoach as dotRoach
import drpActor.utils.tasks as drpTasks
import lsst.daf.persistence as dafPersist

reload(cmdList)
reload(dotRoach)


class DrpEngine(object):
    def __init__(self, actor, target, CALIB, rerun, pfsConfigDir):
        self.actor = actor
        self.target = target
        self.CALIB = CALIB
        self.rerun = rerun
        self.pfsConfigDir = pfsConfigDir

        self.fileBuffer = []
        self.dotRoach = None
        self.defects = dict()

        # default settings
        self.doAutoIngest = True
        self.ingestMode = 'link'
        self.ingestFlavour = cmdList.Ingest
        self.doAutoDetrend = True
        self.doAutoReduce = False

        # for hilo base only.
        if self.actor.site == 'HILO':
            self.ingestMode = 'copy'
            self.ingestFlavour = cmdList.IngestPgsql
            self.doAutoDetrend = False
            self.doAutoReduce = True

        self.butler = self.loadButler()

    @classmethod
    def fromConfigFile(cls, actor):
        return cls(actor, **actor.actorConfig[actor.site])

    def addFile(self, file):
        """ Add new file into the buffer. """
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
            except:
                return False

        md = self.butler.get('raw_md', **file.dataId)
        return dict(windowed=isWindowed(md))

    def ingestPerNight(self, filesPerNight):
        """ ingest multiple files at the same time."""
        if all([file.ingested for file in filesPerNight]):
            return

        for file in filesPerNight:
            file.ingested = True

        return self.ingestFlavour(self.target, filesPerNight[0].starPath, pfsConfigDir=self.pfsConfigDir,
                                  mode=self.ingestMode)

    def isrRemoval(self, visit):
        """ Proceed with isr removal for that visit."""

        def genCommandStatus(cmd, status, statusStr, timing):
            """ Just generate simple status keyword for each visit."""
            self.actor.bcast.inform(f'{cmd}Status={visit},{status},{statusStr},{timing}')

        # get exposure with same visit
        files = [file for file in self.fileBuffer if file.visit == visit]
        if not files:
            return

        nights = list(set([file.night for file in files]))
        [visit] = list(set([file.visit for file in files]))

        if self.doAutoIngest:
            # seems unlikely to have separate night for one visit for still possible.
            for night in nights:
                filesPerNight = [file for file in files if file.night == night]
                cmd = self.ingestPerNight(filesPerNight)
                genCommandStatus('ingest', *cmd.run())

        nirFiles = [file for file in files if file.arm == 'n']
        ccdFiles = list(set(files) - set(nirFiles))

        if self.doAutoDetrend:
            if ccdFiles:
                options = self.lookupMetaData(files[0])
                cmd = cmdList.Detrend(self.target, self.CALIB, self.rerun, visit, **options)
                genCommandStatus('detrend', *cmd.run())
                self.genFilesKeywordForDisplay()

            for file in nirFiles:
                raw = self.butler.get('raw', dataId=file.dataId)
                defects = self.getDefects(file.dataId)
                calexp = drpTasks.doIPCTask.run(raw, defects=defects).exposure
                self.butler.put(calexp, 'calexp', file.dataId)

        if self.doAutoReduce:
            if ccdFiles:
                options = self.lookupMetaData(files[0])
                cmd = cmdList.ReduceExposure(self.target, self.CALIB, self.rerun, visit, **options)
                genCommandStatus('reduceExposure', *cmd.run())

        if self.dotRoach is not None:
            self.dotRoach.runAway(files)
            self.dotRoach.status(cmd=self.actor.bcast)

    def genFilesKeywordForDisplay(self):
        """ Generate detrend keyword for isr"""
        toRemove = []
        for file in self.fileBuffer:

            try:
                isrPath = self.butler.getUri('calexp', arm=file.arm, visit=file.visit)
                toRemove.append(file)
            except:
                isrPath = False

            self.actor.bcast.inform(f'detrend={isrPath}')

        for file in toRemove:
            self.fileBuffer.remove(file)

    def newPfsDesign(self, designId):
        """New PfsDesign has been declared, copy it to the repo if new."""
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
            logging.info(f'loading defects for {cameraKey}')
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
