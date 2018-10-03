#!/usr/bin/env python

from functools import partial
import os
from actorcore.Actor import Actor
from twisted.internet import reactor


class DrpActor(Actor):
    def __init__(self, name, productName=None, configFile=None, debugLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        cams = ['b1', 'r1']

        Actor.__init__(self, name,
                       productName=productName,
                       configFile=configFile, modelNames=['ccd_%s' % cam for cam in cams])

        reactor.callLater(5, partial(self.attachCallbacks, cams))
        self.drpFolder = 'test'

    def attachCallbacks(self, cams):
        self.logger.info('attaching callbacks cams=%s' % (','.join(cams)))

        for cam in cams:
            self.models['ccd_%s' % cam].keyVarDict['filepath'].addCallback(self.newFilepath, callNow=False)

    def newFilepath(self, keyvar):
        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        filepath = os.path.join(root, 'pfs', night, fname)
        self.callCommand('ingest filepath=%s' % filepath)
        #self.callCommand('detrend filepath=%s' % filepath)


def main():
    actor = DrpActor('drp', productName='drpActor')
    actor.run()


if __name__ == '__main__':
    main()
