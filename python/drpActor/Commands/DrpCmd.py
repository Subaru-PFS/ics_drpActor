#!/usr/bin/env python
import os
import time
from importlib import reload

import drpActor.detrend as detrend
import lsst.daf.persistence as dafPersist
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.detrend import doDetrend
from drpActor.ingest import doIngest
from drpActor.utils import imgPath, getInfo

reload(detrend)


class DrpCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        self.butler = None

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
        return self.actor.config.get(self.actor.site, 'target').strip()

    @property
    def rerun(self):
        return self.actor.config.get(self.actor.site, 'rerun').strip()

    @property
    def pfsConfigDir(self):
        return self.actor.config.get(self.actor.site, 'pfsConfigDir').strip()

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
        doIngest(filepath, target=target, pfsConfigDir=self.pfsConfigDir)

        cmd.inform(f"ingest={filepath}")
        if doFinish:
            cmd.finish()

    def detrend(self, cmd, doFinish=True):
        cmdKeys = cmd.cmd.keywords

        visit = cmdKeys["visit"].values[0]
        arm = cmdKeys["arm"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.rerun

        if 'target' in cmdKeys or 'rerun' in cmdKeys:
            butler = dafPersist.Butler(os.path.join(target, 'rerun', rerun, 'detrend'))
        else:
            butler = self.loadButler()

        if butler is None:
            cmd.inform('text="butler not loaded, detrending one frame now ...')
            doDetrend(target, rerun, visit)
            butler = self.loadButler()

        if not imgPath(butler, visit, arm):
            cmd.debug('text="detrend cmd started on %s"' % (visit))
            doDetrend(target, rerun, visit)

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
        cmd.finish(f"detrend={fullPath}; text='ran in {t1 - t0:0.4f}s'")

    def loadButler(self):
        """load butler. """
        if self.butler is None:
            try:
                butler = dafPersist.Butler(os.path.join(self.target, 'rerun', self.rerun, 'detrend'))
            except Exception as e:
                self.actor.logger.warning('text=%s' % self.actor.strTraceback(e))
                self.actor.logger.warning('butler could not be instantiated, might be a fresh one')
                butler = None

            self.butler = butler

        return self.butler
