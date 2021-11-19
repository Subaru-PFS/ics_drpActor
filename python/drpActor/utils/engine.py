import os

import drpActor.utils.detrend as detrend
import drpActor.utils.ingest as ingest
import lsst.daf.persistence as dafPersist


class DrpEngine(object):
    def __init__(self, actor, target, CALIB, rerun, pfsConfigDir):
        self.actor = actor
        self.target = target
        self.CALIB = CALIB
        self.rerun = rerun
        self.pfsConfigDir = pfsConfigDir

        self.fileBuffer = []
        self.butler = self.loadButler()

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

        md = self.butler.get('raw_md', visit=file.visit, arm=file.arm, spectrograph=file.specNum)
        return dict(windowed=isWindowed(md))

    def ingestPerNight(self, filesPerNight):
        """ ingest multiple files at the same time."""
        for file in filesPerNight:
            file.ingested = True

        ingest.doIngest(filesPerNight[0].starPath, target=self.target, pfsConfigDir=self.pfsConfigDir)

    def isrRemoval(self, visit):
        """ Proceed with isr removal for that visit."""
        # get exposure with same visit
        files = [file for file in self.fileBuffer if file.visit == visit]
        if not files:
            return

        nights = list(set([file.night for file in files]))
        [visit] = list(set([file.visit for file in files]))

        for night in nights:
            filesPerNight = [file for file in files if file.night == night]
            self.ingestPerNight(filesPerNight)

        options = self.lookupMetaData(files[0])
        detrend.doDetrend(self.target, self.CALIB, self.rerun, visit, **options)

        self.genKeywordForDisplay()

    def genKeywordForDisplay(self):
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
