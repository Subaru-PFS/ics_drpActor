#!/usr/bin/env python
import os
from importlib import reload
import time

import drpActor.detrend as detrend
import lsst.daf.persistence as dafPersist
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.ingest import doIngest
from drpActor.utils import imgPath, getInfo

reload(detrend)


class DrpCmd(object):
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
            ('detrend', '<visit> <arm> [<rerun>]', self.detrend),
            ('process', '<filepath>  [<target>] [<rerun>]', self.process),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("target", types.String(), help="target drp folder for ingest"),
                                        keys.Key("rerun", types.String(), help="rerun drp folder"),
                                        keys.Key("filepath", types.String(), help="Raw FITS File path"),
                                        keys.Key("arm", types.String(), help="arm"),
                                        keys.Key("visit", types.Int(), help="visitId"),
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

    def ingest(self, cmd, doFinish=True):
        cmdKeys = cmd.cmd.keywords

        filepath = cmdKeys["filepath"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        cmd.debug('text="ingest cmd started on %s"' % (filepath))
        doIngest(filepath, target)

        cmd.inform(f"ingest={filepath}")
        if doFinish:
            cmd.finish()

    def detrend(self, cmd, doFinish=True):
        cmdKeys = cmd.cmd.keywords

        visit = cmdKeys["visit"].values[0]
        arm = cmdKeys["arm"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.rerun
        butler = dafPersist.Butler(os.path.join(target, 'rerun', rerun, 'detrend'))

        cmd.debug('text="detrend cmd started on %s"' % (visit))
        if not imgPath(butler, visit, arm):
            detrend.doDetrend(visit=visit, target=target, rerun=rerun)

        fullPath = imgPath(butler, visit, arm)
        cmd.inform(f"detrend={fullPath}")
        if doFinish:
            cmd.finish()

    def process(self, cmd):
        """Entirely process a single new exposure. """

        cmdKeys = cmd.cmd.keywords
        filepath = cmdKeys["filepath"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.rerun

        visit, arm = getInfo(filepath)

        self.ingest(cmd, doFinish=False)

        t0 = time.time()
        cmd.debug('text="detrend started on %s"' % (visit))
        butler = dafPersist.Butler(os.path.join(target, 'rerun', rerun, 'detrend'))
        if not imgPath(butler, visit, arm):
            detrend.doDetrend(visit=visit, arm=arm, target=target, rerun=rerun)
        t1 = time.time()

        fullPath = imgPath(butler, visit, arm)
        cmd.finish(f"detrend={fullPath}; text='ran in {t1-t0:0.4f}s'")
