#!/usr/bin/env python
import opscore.protocols.keys as keys
import opscore.protocols.types as types
import os
import time
from importlib import reload
from astropy.io import fits
from drpActor.myIngestTask import MyIngestTask

import drpActor.script as script
reload(script)


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
            ('ingest', '<filepath>', self.ingest),
            ('detrend', '<filepath> [<drpFolder>]', self.detrend),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("expTime", types.Float(), help="The exposure time"),
                                        keys.Key("drpFolder", types.String(), help="custom drp folder"),
                                        keys.Key("filepath", types.String(), help="FITS File path"),

                                        )

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
        myIngest = MyIngestTask()
        cmd.inform("text='%s'" % myIngest.customIngest(filepath))
        cmd.finish("ingest=%s'" % filepath)

    def detrend(self, cmd):
        t0 = time.time()
        cmdKeys = cmd.cmd.keywords

        filepath = cmdKeys["filepath"].values[0]
        hdulist = fits.open(filepath)
        nightFolder = hdulist[0].header['DATE-OBS'][:10]
        __, filename = os.path.split(filepath)

        prefix = filename[:4]
        visitId = int(filename[4:10])
        specId = int(filename[10])
        arm = self.armNum[filename[11]]
        cam = '%s%d' % (arm, specId)
        target = '/drp/%s' % cam
        rerun = "ginga"

        script.doDetrend(visit=visitId, rerun=rerun, target=target)

        detrendPath = os.path.join(target, 'rerun', rerun, 'detrend/calExp', nightFolder,
                                   'v%s' % str(visitId).zfill(7))

        fullPath = '%s/%s%s%s%s.fits' % (detrendPath, 'calExp-LA', str(visitId).zfill(6), arm, specId)

        while not os.path.isfile(fullPath):
            if time.time() - t0 > 30:
                raise TimeoutError('file has not been created')
            time.sleep(1)

        cmd.finish('detrend=%s' % fullPath)
