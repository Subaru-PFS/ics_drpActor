#!/usr/bin/env python
import os
import time
from importlib import reload

import drpActor.detrend as detrend
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from astropy.io import fits
from drpActor.myIngestTask import MyIngestTask

reload(detrend)


class DrpCmd(object):
    armNum = {'1': 'b',
              '2': 'r',
              '3': 'n',
              '4': 'm'}

    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('ping', '', self.ping),
            ('status', '', self.status),
            ('ingest', '<filepath> [<target>]', self.ingest),
            ('detrend', '<filepath> [<rerun>]', self.detrend),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("target", types.String(), help="target drp folder"),
                                        keys.Key("rerun", types.String(), help="rerun drp folder"),
                                        keys.Key("filepath", types.String(), help="FITS File path"),
                                        )

    @property
    def target(self):
        return self.actor.config.get('drp', 'target').strip()

    @property
    def rerun(self):
        return self.actor.config.get('drp', 'rerun').strip()

    def ping(self, cmd):
        """Query the actor for liveness/happiness."""
        cmd.finish("text='Present and (probably) well'")

    def status(self, cmd):
        """Report status and version; obtain and send current data"""

        cmd.inform('text="Present!"')
        cmd.finish()

    def ingest(self, cmd):
        cmdKeys = cmd.cmd.keywords

        filepath = cmdKeys["filepath"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        myIngest = MyIngestTask()
        cmd.inform("text='%s'" % myIngest.customIngest(filepath, target))
        cmd.finish("ingest=%s'" % filepath)

    def detrend(self, cmd):
        t0 = time.time()
        cmdKeys = cmd.cmd.keywords

        filepath = cmdKeys["filepath"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.rerun
        hdulist = fits.open(filepath)
        nightFolder = hdulist[0].header['DATE-OBS'][:10]
        __, filename = os.path.split(filepath)

        visitId = int(filename[4:10])
        specId = int(filename[10])
        arm = self.armNum[filename[11]]

        detrendPath = os.path.join(target, 'rerun', rerun, 'detrend/calExp', nightFolder,
                                   'v%s' % str(visitId).zfill(7))

        fullPath = '%s/%s%s%s%s.fits' % (detrendPath, 'calExp-SA', str(visitId).zfill(6), arm, specId)

        if not os.path.isfile(fullPath):
            detrend.doDetrend(visit=visitId, target=target, rerun=rerun)

        while not os.path.isfile(fullPath):
            if time.time() - t0 > 45:
                raise TimeoutError('file has not been created')
            time.sleep(1)

        cmd.finish('detrend=%s' % fullPath)
