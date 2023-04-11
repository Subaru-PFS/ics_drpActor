#!/usr/bin/env python


import argparse
import logging
import os
import threading
from importlib import reload

import drpActor.utils.engine as drpEngine
from actorcore.Actor import Actor
from drpActor.utils.files import CCDFile, HxFile
from ics.utils.sps.spectroIds import getSite
from twisted.internet import reactor


class DrpActor(Actor):
    allSites = dict(L='LAM', S='SUBARU', Z='HILO')
    nHandlers = 2  # [<StreamHandler <stderr> (INFO)>, <OpsRotatingFileHandler /data/logs/actors/drp/$DATE.log (DEBUG)>]

    def __init__(self, name, productName=None, configFile=None, logLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        specIds = list(range(1, 5))
        vis = [f'b{specId}' for specId in specIds] + [f'r{specId}' for specId in specIds]
        nir = [f'n{specId}' for specId in specIds]

        self.ccds = [f'ccd_{cam}' for cam in vis]
        self.hxs = [f'hx_{cam}' for cam in nir]

        self.site = DrpActor.allSites[getSite()][0]

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
            self.models['iic'].keyVarDict['designId'].addCallback(self.newPfsDesign, callNow=False)

            self.engine = self.loadDrpEngine()
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
        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        self.logger.info(f'newfilepath: {root}, {night}, {fname} ; threads={threading.active_count()}')
        self.engine.newExposure(CCDFile(root, night, fname))

    def hxFilepath(self, keyvar):
        """ CCD Filepath callback"""
        try:
            filepath = keyvar.getValue()
        except ValueError:
            return

        rootNight, fname = os.path.split(filepath)
        root, night = os.path.split(rootNight)

        self.logger.info(f'newfilepath: {filepath} ; threads={threading.active_count()}')
        self.engine.newExposure(HxFile(root, night, fname))

    def spsFileIds(self, keyvar):
        """ spsFileIds callback. """
        try:
            [visit, camList, camMask] = keyvar.getValue()
        except ValueError:
            return

        reactor.callLater(0.2, self.engine.newVisit, visit)

    def newPfsDesign(self, keyvar):
        try:
            designId = keyvar.getValue()
        except ValueError:
            return

        self.engine.newPfsDesign(designId)

    def cleanLogHandlers(self):
        """Drp argument parser add some unwanted handlers, get rid of those."""
        logger = logging.getLogger()

        while len(logger.handlers) > DrpActor.nHandlers:
            logger.info(f'removing logger handler : {logger.handlers[DrpActor.nHandlers]}')
            logger.removeHandler(logger.handlers[DrpActor.nHandlers])


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
