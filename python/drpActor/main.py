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
from functools import partial

from actorcore.Actor import Actor
from drpActor.utils import getInfo
from pfs.utils.spectroIds import getSite
from twisted.internet import reactor


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

    def connectionMade(self):
        """Called when the actor connection has been established: wire in callbacks."""

        if self.everConnected is False:
            self.logger.info('attaching callbacks cams=%s' % (','.join(self.cams)))

            for cam in self.cams:
                self.models['ccd_%s' % cam].keyVarDict['filepath'].addCallback(self.newFilepath, callNow=False)
            self.everConnected = True

    def newFilepath(self, keyvar):
        try:
            [root, night, fname] = keyvar.getValue()
        except ValueError:
            return

        self.logger.info(f'newfilepath: {root}, {night}, {fname}. threads={threading.active_count()}')

        filepath = os.path.join(root, night, 'sps', fname)
        # self.callCommand('process filepath=%s' % filepath)
        self.callCommand('ingest filepath=%s' % filepath)
        visit, arm = getInfo(filepath)
        reactor.callLater(5, partial(self.callCommand, f'detrend visit={visit} arm={arm}'))


def main():
    actor = DrpActor('drp', productName='drpActor')
    actor.run()


if __name__ == '__main__':
    main()
