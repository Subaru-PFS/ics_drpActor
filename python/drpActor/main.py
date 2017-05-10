#!/usr/bin/env python

from actorcore.Actor import Actor
from actorcore.QThread import QThread


class DrpActor(Actor):
    def __init__(self, name, productName=None, configFile=None, debugLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        Actor.__init__(self, name,
                       productName=productName,
                       configFile=configFile, modelNames=['ccd_r1'])

        self.allThreads = {}
        self.lastFilepath = None
        self.drpFolder = "test"

        drpCheck = QThread(self, "drpCheck")
        drpCheck.handleTimeout = self.checkExp
        self.allThreads["drpCheck"] = drpCheck
        drpCheck.start()

        drpProcess = QThread(self, "drpProcess")
        drpProcess.handleTimeout = self.sleep
        self.allThreads["drpProcess"] = drpProcess
        drpProcess.start()

    def checkFilepath(self):
        ccdKeys = self.models['ccd_r1']
        [root, night, fname] = ccdKeys.keyVarDict['filepath'].getValue()

        return "%s/pfs/%s/%s" % (root, night, fname)

    def checkExp(self):
        filepath = self.checkFilepath()
        self.lastFilepath = filepath if self.lastFilepath is None else self.lastFilepath

        if filepath != self.lastFilepath:
            self.callCommand("ingest fitsPath=%s " % filepath)
            self.callCommand("detrend fitsPath=%s context=%s" % (filepath, self.drpFolder))
            self.lastFilepath = filepath

    def sleep(self):
        pass


def main():
    actor = DrpActor('drp', productName='drpActor')
    actor.run()


if __name__ == '__main__':
    main()
