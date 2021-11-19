import drpActor.detrend as detrend
# import drpActor.utils.dotroaches as dotroaches
import lsst.daf.persistence as dafPersist
import opscore.protocols.keys as keys
import opscore.protocols.types as types
import os
import time
from drpActor.detrend import doDetrend
from drpActor.ingest import doIngest
from drpActor.utils import imgPath, getInfo
from importlib import reload
import pandas as pd
import numpy as np

reload(detrend)
# reload(dotroaches)


class DrpCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        self.butler = None
        self.doStartLoop = False
        self.lastVisit = None

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
            ('startDotLoop', '', self.startDotLoop),
            ('processDotData', '', self.processDotData),
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
    def CALIB(self):
        return self.actor.config.get(self.actor.site, 'CALIB').strip()

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
        self.lastVisit = visit
        arm = cmdKeys["arm"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.target
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.rerun

        if 'target' in cmdKeys or 'rerun' in cmdKeys:
            butler = dafPersist.Butler(os.path.join(target, 'rerun', rerun, 'detrend'))
        else:
            butler = self.loadButler()

        if butler is None:
            cmd.inform('text="butler not loaded, detrending one frame now ...')
            doDetrend(target, self.CALIB, rerun, visit)
            butler = self.loadButler()

        if not imgPath(butler, visit, arm):
            cmd.debug('text="detrend cmd started on %s"' % (visit))
            doDetrend(target, self.CALIB, rerun, visit)

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
                butler = dafPersist.Butler(os.path.join(self.target, 'rerun', self.rerun))
            except Exception as e:
                self.actor.logger.warning('text=%s' % self.actor.strTraceback(e))
                self.actor.logger.warning('butler could not be instantiated, might be a fresh one')
                butler = None

            self.butler = butler

        return self.butler

    def processDotData(self, cmd):
        """startFpsLoop. """
        cmdKeys = cmd.cmd.keywords
        visit = self.lastVisit

        dataId = dict(visit=visit, arm="r", spectrograph=1)
        calexp = self.butler.get('calexp', dataId)
        df = dotroaches.robustFluxEstimation(calexp.image.array)
        df['visit'] = visit

        if self.doStartLoop:
            filename = f'dotroaches{str(visit).zfill(6)}.csv'
            self.filepath = os.path.join('.', filename)
            df.to_csv(self.filepath)
            self.doStartLoop = False

        else:
            allDf = pd.read_csv(self.filepath, index_col=0)
            last = allDf.query(f'visit=={allDf.visit.max()}').sort_values('cobraId')
            fluxGradient = df.centerFlux.to_numpy() - last.centerFlux.to_numpy()
            keepMoving = fluxGradient < 0
            keepMoving = np.logical_and(last.keepMoving, keepMoving).to_numpy()
            df['fluxGradient'] = fluxGradient
            df['keepMoving'] = keepMoving
            allDf = pd.concat([allDf, df]).reset_index(drop=True)
            allDf.to_csv(self.filepath)

        cmd.finish(f'fpsDotData={self.filepath}')

    def startDotLoop(self, cmd):
        self.doStartLoop = True
        cmd.finish('text="starting do loop, run dotroaches, run ! "')
