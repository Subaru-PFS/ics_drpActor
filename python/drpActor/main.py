#!/usr/bin/env python


import argparse
import logging
import os
import threading
from importlib import reload

import drpActor.utils.engine as drpEngine
from actorcore.Actor import Actor
from drpActor.utils.files import CCDFile, HxFile, PfsConfigFile
from ics.utils.sps.spectroIds import getSite
from twisted.internet import reactor


class DrpActor(Actor):
    allSites = dict(L='LAM', S='SUBARU', H='HILO')
    nHandlers = 2  # [<StreamHandler <stderr> (INFO)>, <OpsRotatingFileHandler /data/logs/actors/drp/$DATE.log (DEBUG)>]

    def __init__(self, name, productName=None, configFile=None, logLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        specIds = list(range(1, 5))
        vis = [f'b{specId}' for specId in specIds] + [f'r{specId}' for specId in specIds]
        nir = [f'n{specId}' for specId in specIds]

        self.ccds = [f'ccd_{cam}' for cam in vis]
        self.hxs = [f'hx_{cam}' for cam in nir]

        # default return S now.
        site = 'H' if name == 'drp2' else getSite()
        self.site = site
        self.engine = None

        Actor.__init__(self, name,
                       productName=productName,
                       configFile=configFile, modelNames=self.ccds + self.hxs + ['sps', 'iic'])

        self.everConnected = False

    def connectionMade(self):
        """Called when the actor connection has been established: wire in callbacks."""
        if self.everConnected is False:

            for ccd in self.ccds:
                self.models[ccd].keyVarDict['filepath'].addCallback(self.ccdFilepath, callNow=False)
                self.logger.info(f'{ccd}.filepath callback attached')

            for hx in self.hxs:
                self.models[hx].keyVarDict['filename'].addCallback(self.hxFilepath, callNow=False)
                self.logger.info(f'{hx}.filename callback attached')

            self.models['sps'].keyVarDict['fileIds'].addCallback(self.spsFileIds, callNow=False)
            self.models['iic'].keyVarDict['pfsConfig'].addCallback(self.newPfsConfig, callNow=False)

            self.everConnected = True

    def loadDrpEngine(self):
        """ Return DrpEngine object from config file."""
        reload(drpEngine)
        return drpEngine.DrpEngine.fromConfigFile(self)

    def reloadConfiguration(self, cmd):
        """ reload butler"""
        self.engine = self.loadDrpEngine()

    def ccdFilepath(self, keyvar):
        """ CCD Filepath callback"""
        if not self.engine:
            return

        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        self.engine.newExposure(CCDFile(root, night, fname))

    def hxFilepath(self, keyvar):
        """ CCD Filepath callback"""
        if not self.engine:
            return

        try:
            filepath = keyvar.getValue()
        except ValueError:
            return

        rootNightType, fname = os.path.split(filepath)
        rootNight, fitsType = os.path.split(rootNightType)

        self.engine.newExposure(HxFile(rootNight, fitsType, fname))

    def spsFileIds(self, keyvar):
        """ spsFileIds callback. """
        if not self.engine:
            return

        try:
            [visit, camList, camMask] = keyvar.getValue()
        except ValueError:
            return

        reactor.callLater(0.2, self.engine.newVisit, visit)

    def newPfsConfig(self, keyvar):
        """pfsConfigFinalized callback."""
        if not self.engine:
            return

        try:
            [designId, visit, dateDir, ra, dec, pa, designName, designId0, variant, instStatus] = keyvar.getValue()
        except ValueError:
            return

        self.engine.newPfsConfig(PfsConfigFile.fromKeys(designId, visit, dateDir))


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
