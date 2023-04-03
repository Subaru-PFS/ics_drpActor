import os
from importlib import reload

import drpActor.utils.cmdList as cmdList
import drpActor.utils.dotRoach as dotRoach
import drpActor.utils.tasks.ingest as ingestTask
import drpActor.utils.tasks.tasksExec as tasksExec
import lsst.daf.persistence as dafPersist

reload(cmdList)
reload(dotRoach)


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

        # default settings
        self.doCopyDesignToPfsConfigDir = False
        self.doAutoIngest = True
        self.ingestMode = 'link'
        self.ingestFlavour = ingestTask.IngestTask
        self.doAutoDetrend = True
        self.doAutoReduce = False

        self.tasks = tasksExec.TasksExec(self)

        # for hilo base only.
        if self.actor.site == 'H':
            self.ingestFlavour = ingestTask.PgsqlIngestTask
            self.ingestMode = 'copy'
            self.doAutoDetrend = False
            self.doAutoReduce = True

        self.butler = self.loadButler()

    @property
    def logger(self):
        return self.actor.logger

    @classmethod
    def fromConfigFile(cls, actor):
        return cls(actor, **actor.actorConfig[actor.site])

    def loadButler(self):
        """load butler. """
        try:
            butler = dafPersist.Butler(os.path.join(self.target, 'rerun', self.rerun))
        except Exception as e:
            self.logger.warning('text=%s' % self.actor.strTraceback(e))
            self.logger.warning('butler could not be instantiated, might be a fresh one')
            butler = None

        return butler

    def newExposure(self, file):
        """ Add new file into the buffer. """
        # Assessing what has been done on this file/dataId .
        file.initialize(self.butler)
        self.logger.info(f'id : {str(file.dataId)} ingested={file.ingested} calexp={file.calexp} pfsArm={file.pfsArm}')
        self.fileBuffer.append(file)

        if self.doAutoIngest and not file.ingested:
            self.tasks.ingest(file)

        if self.doAutoDetrend and file.ingested:
            if file.calexp:
                self.genDetrendKey(file)
            else:
                self.tasks.detrend(file)

        if self.doAutoReduce and file.ingested:
            self.tasks.reduceExposure(file)

    def genDetrendKey(self, file, cmd=None):
        """Generate detrend key to display image with gingaActor."""
        cmd = self.actor.bcast if cmd is None else cmd

        if file.calexp:
            cmd.inform(f"detrend={file.calexp}")

        self.genDetrendStatus(file.visit, cmd=cmd)

    def genIngestStatus(self, visit, cmd=None):
        """Generate ingestStatus key."""
        cmd = self.actor.bcast if cmd is None else cmd

        ingested = [file.ingested for file in self.fileBuffer if file.visit == visit]
        statusStr = 'OK' if all(ingested) else 'FAILED'
        retCode = 0 if all(ingested) else -1

        cmd.inform(f'ingestStatus={visit},{retCode},{statusStr},{0}')

    def genDetrendStatus(self, visit, cmd=None):
        """Generate detrendStatus key."""
        cmd = self.actor.bcast if cmd is None else cmd

        sameVisit = [file for file in self.fileBuffer if file.visit == visit]
        idle = [file.state == 'idle' for file in sameVisit]

        if all(idle):
            calexp = [file.calexp for file in sameVisit]
            statusStr = 'OK' if all(calexp) else 'FAILED'
            retCode = 0 if all(calexp) else -1

            cmd.inform(f'detrendStatus={visit},{retCode},{statusStr},{0}')

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

    def newPfsDesign(self, designId):
        """New PfsDesign has been declared, copy it to the repo if new."""
        if not self.doCopyDesignToPfsConfigDir:
            return
    #
    #     fileName = f'pfsDesign-0x{designId:016x}.fits'
    #
    #     if not os.path.isfile(os.path.join(self.pfsConfigDir, fileName)):
    #         designPath = os.path.join('/data/pfsDesign', fileName)
    #         try:
    #             shutil.copy(designPath, self.pfsConfigDir)
    #             self.logger.info(f'{fileName} copied {self.pfsConfigDir} successfully !')
    #         except Exception as e:
    #             self.logger.warning(f'failed to copy from {designPath} to {self.pfsConfigDir} with {e}')
