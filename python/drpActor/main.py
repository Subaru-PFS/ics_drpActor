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

import argparse
import logging
import threading

from actorcore.Actor import Actor
from drpActor.utils.engine import DrpEngine
from drpActor.utils.files import CCDFile
from ics.utils.sps.spectroIds import getSite
from twisted.internet import reactor


class DrpActor(Actor):
    allSites = dict(L='LAM', S='SUBARU', Z='HILO')

    def __init__(self, name, productName=None, configFile=None, logLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        specIds = list(range(1, 5))
        self.cams = [f'b{specId}' for specId in specIds] + [f'r{specId}' for specId in specIds]
        self.site = DrpActor.allSites[getSite()]

        Actor.__init__(self, name,
                       productName=productName,
                       configFile=configFile, modelNames=['ccd_%s' % cam for cam in self.cams] + ['sps'])

        self.everConnected = False

    def connectionMade(self):
        """Called when the actor connection has been established: wire in callbacks."""
        if self.everConnected is False:
            self.logger.info('attaching callbacks cams=%s' % (','.join(self.cams)))

            for cam in self.cams:
                self.models['ccd_%s' % cam].keyVarDict['filepath'].addCallback(self.ccdFilepath, callNow=False)

            self.models['sps'].keyVarDict['fileIds'].addCallback(self.spsFileIds, callNow=False)
            self.engine = self.loadDrpEngine()
            self.everConnected = True

    def loadDrpEngine(self):
        """ """
        return DrpEngine(self, target=self.config.get(self.site, 'target').strip(),
                         CALIB=self.config.get(self.site, 'CALIB').strip(),
                         rerun=self.config.get(self.site, 'rerun').strip(),
                         pfsConfigDir=self.config.get(self.site, 'pfsConfigDir').strip())

    def reloadConfiguration(self, cmd):
        """ reload butler"""
        self.engine = self.loadDrpEngine()

    def ccdFilepath(self, keyvar):
        """ CCD Filepath callback"""
        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        self.logger.info(f'newfilepath: {root}, {night}, {fname}. threads={threading.active_count()}')

        self.engine.addFile(CCDFile(root, night, fname))
        reactor.callLater(5, self.checkLeftOver)

    def spsFileIds(self, keyvar):
        """ spsFileIds callback. """
        try:
            [visit, camList, camMask] = keyvar.getValue()
        except ValueError:
            return

        self.engine.isrRemoval(visit)

    def checkLeftOver(self):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=None, type=str, nargs='?',
                        help='configuration file to use')
    parser.add_argument('--logLevel', default=logging.INFO, type=int, nargs='?',
                        help='logging level')
    parser.add_argument('--name', default='drp', type=str, nargs='?',
                        help='identity')
    args = parser.parse_args()

    theActor = DrpActor(args.name,
                        productName='drpActor',
                        configFile=args.config,
                        logLevel=args.logLevel)
    theActor.run()


if __name__ == '__main__':
    main()
