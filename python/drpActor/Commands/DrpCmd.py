#!/usr/bin/env python
import os
import time

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from astropy.io import fits
from drpActor.myIngestTask import MyIngestTask
from drpActor.utils import threaded
from lsst.obs.pfs.detrendTask import DetrendTask


class DrpCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.name = 'drpProcess'
        self.vocab = [
            ('ping', '', self.ping),
            ('status', '', self.status),
            ('ingest', '<fitsPath>', self.ingest),
            ('detrend', '[<context>] <fitsPath>', self.detrend),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("expTime", types.Float(), help="The exposure time"),
                                        keys.Key("context", types.String(), help="custom drp folder"),
                                        keys.Key("fitsPath", types.String(), help="FITS File path"),

                                        )

    def ping(self, cmd):
        """Query the actor for liveness/happiness."""
        cmd.finish("text='Present and (probably) well'")

    def status(self, cmd):
        """Report status and version; obtain and send current data"""

        cmd.inform('text="Present!"')
        cmd.finish()

    @threaded
    def ingest(self, cmd):
        cmdKeys = cmd.cmd.keywords

        fitsPath = cmdKeys["fitsPath"].values[0]
        myIngest = MyIngestTask()
        cmd.inform("text='%s'" % myIngest.customIngest(fitsPath))
        cmd.finish("ingest=%s'" % fitsPath)

    @threaded
    def detrend(self, cmd):
        cmdKeys = cmd.cmd.keywords

        fitsPath = cmdKeys["fitsPath"].values[0]
        hdulist = fits.open(fitsPath)
        imgFolder = hdulist[0].header['DATE-OBS'][:10]
        imgNumber = fitsPath.split('/')[-1][4:10]
        id_visit = int(imgNumber)

        args = ['/drp/lam', '-c', 'isr.doDark=False', '-c', 'isr.doFlat=False', '--rerun']
        context = 'pfs/%s' % cmdKeys["context"].values[0] if "context" in cmdKeys else 'pfs'
        args.append(context)
        args.extend(['--id', 'visit=%i' % id_visit, "--no-version"])

        detrendTask = DetrendTask()
        detrendTask.parseAndRun(args=args)
        detrendPath = '/drp/lam/rerun/%s/postISRCCD/%s/v%s' % (context, imgFolder, imgNumber.zfill(7))

        try:
            detrendPath, b, fname = self.getFilename(detrendPath)
            cmd.finish("detrend=%s/%s'" % (detrendPath, fname[-1]))
        except:
            cmd.fail("text='file not created'")

    def getFilename(self, detrendPath, i=0):
        time.sleep(0.2)
        print detrendPath
        try:
            return next(os.walk(detrendPath))
        except:
            if i < 200:
                return self.getFilename(detrendPath, i + 1)
            else:
                raise
