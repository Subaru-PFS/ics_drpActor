#!/usr/bin/env python

# Monkey patch twisted version mechanism, which conflicts with
# lsst.base.packages.getPythonPackages()
#
# MUST be called before any twisted import, which both we and
# actorcore.Actor do.
#
def _monkeyPatchIncremental():
    import incremental

    incremental.Version.saved_cmp = incremental.Version.__cmp__

    def __monkeyCmp__(self, other):
        if isinstance(other, str):
            a = self.public()
            b = other
            return (a > b) - (a < b)
        return self.saved_cmp(other)

    incremental.Version.__cmp__ = __monkeyCmp__


_monkeyPatchIncremental()

import os
import threading

from actorcore.Actor import Actor
from ics.utils.sps.spectroIds import getSite, SpectroIds
from twisted.internet import reactor


class CCDFile(object):
    fromArmNum = dict([(v, k) for k, v in SpectroIds.validArms.items()])

    def __init__(self, root, night, filename):
        self.root = root
        self.night = night
        self.filename = filename
        self.visit = int(filename[4:10])
        self.specNum = int(filename[10])
        self.armNum = int(filename[11])
        self.arm = CCDFile.fromArmNum[self.armNum]

        self.ingested = False
        self.detrended = False

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, 'sps', self.filename)

    @property
    def starPath(self):
        return os.path.join(self.root, self.night, 'sps', f'{self.filename[:10]}*.fits')


class DrpActor(Actor):
    allSites = dict(L='LAM', S='SUBARU')

    def __init__(self, name, productName=None, configFile=None, debugLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        specIds = list(range(1, 5))
        self.cams = [f'b{specId}' for specId in specIds] + [f'r{specId}' for specId in specIds]
        self.site = DrpActor.allSites[getSite()]

        Actor.__init__(self, name,
                       productName=productName,
                       configFile=configFile, modelNames=['ccd_%s' % cam for cam in self.cams])

        self.everConnected = False
        self.exposureBuffer = []

    def connectionMade(self):
        """Called when the actor connection has been established: wire in callbacks."""

        if self.everConnected is False:
            self.logger.info('attaching callbacks cams=%s' % (','.join(self.cams)))

            for cam in self.cams:
                self.models['ccd_%s' % cam].keyVarDict['filepath'].addCallback(self.newFilepath, callNow=False)

            self.models['sps'].keyVarDict['fileIds'].addCallback(self.spsFileIds, callNow=False)
            self.everConnected = True

    def newFilepath(self, keyvar):
        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        self.logger.info(f'newfilepath: {root}, {night}, {fname}. threads={threading.active_count()}')

        self.exposureBuffer.append(CCDFile(root, night, fname))
        reactor.callLater(5, self.checkLeftOver)

    def checkLeftOver(self):
        pass

    def spsFileIds(self, keyvar):
        # reactor.callLater(5, partial(self.callCommand, f'detrend visit={visit} arm={arm}'))

        try:
            [visit, camList, camMask] = keyvar.getValue()
        except ValueError:
            return

        # get exposure with same visit
        files = [file for file in self.exposureBuffer if file.visit == visit]
        if not files:
            return

        nights = list(set([file.night for file in files]))
        for night in nights:
            filesPerNight = [file for file in files if file.night == night]
            for file in filesPerNight:
                file.ingested = True
            startPath = files[0].starPath
            self.callCommand('ingest filepath=%s' % startPath)


def main():
    actor = DrpActor('drp', productName='drpActor')
    actor.run()


if __name__ == '__main__':
    main()
