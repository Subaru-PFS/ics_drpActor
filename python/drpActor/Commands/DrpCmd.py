import os
from importlib import reload

import drpActor.utils.cmdList as cmdList
import drpActor.utils.dotRoach as dotRoach
import lsst.daf.persistence as dafPersist
import opscore.protocols.keys as keys
import opscore.protocols.types as types

reload(dotRoach)
reload(cmdList)


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
            ('ingest', '<filepath>', self.ingest),
            ('detrend', '<visit> <arm> [<rerun>]', self.detrend),

            ('startDotRoach', '<dataRoot> <maskFile> [@(keepMoving)]', self.startDotRoach),
            ('stopDotRoach', '', self.stopDotRoach),
            ('processDotRoach', '', self.processDotRoach),

            ('checkLeftOvers', '', self.checkLeftOvers)
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("rerun", types.String(), help="rerun drp folder"),
                                        keys.Key("filepath", types.String(), help="Raw FITS File path"),
                                        keys.Key("arm", types.String(), help="arm"),
                                        keys.Key("visit", types.Int(), help="visitId"),
                                        keys.Key("dataRoot", types.String(),
                                                 help="dataRoot which will contain the generated outputs"),
                                        keys.Key("maskFile", types.String(),
                                                 help="config file describing which cobras will be put behind dots."),
                                        )

    @property
    def engine(self):
        return self.actor.engine

    def ping(self, cmd):
        """Query the actor for liveness/happiness."""
        cmd.finish("text='Present and (probably) well'")

    def status(self, cmd):
        """Report status and version; obtain and send current data"""
        cmd.inform('text="Present!"')
        cmd.finish()

    def ingest(self, cmd):
        def doIngest(filepath, target, pfsConfigDir, mode):
            drpCmd = cmdList.Ingest(filepath, target=target, pfsConfigDir=pfsConfigDir, mode=mode)
            cmd.debug('text="ingest cmd started on %s"' % (filepath))
            return drpCmd.run()

        cmdKeys = cmd.cmd.keywords
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.engine.target
        pfsConfigDir = cmdKeys["pfsConfigDir"].values[0] if 'pfsConfigDir' in cmdKeys else self.engine.pfsConfigDir
        mode = cmdKeys["mode"].values[0] if 'mode' in cmdKeys else self.engine.ingestMode

        filepath = cmdKeys["filepath"].values[0]
        doIngest(filepath, target, pfsConfigDir, mode)

        cmd.finish(f"ingest={filepath}")

    def detrend(self, cmd):
        def doDetrend(target, CALIB, rerun, visit):
            drpCmd = cmdList.Detrend(target, CALIB, rerun, visit)
            cmd.debug('text="detrend cmd started on %s"' % (visit))
            return drpCmd.run()

        cmdKeys = cmd.cmd.keywords

        visit = cmdKeys["visit"].values[0]
        arm = cmdKeys["arm"].values[0]
        target = cmdKeys["target"].values[0] if 'target' in cmdKeys else self.engine.target
        CALIB = cmdKeys["CALIB"].values[0] if 'CALIB' in cmdKeys else self.engine.CALIB
        rerun = cmdKeys["rerun"].values[0] if 'rerun' in cmdKeys else self.engine.rerun

        doDetrend(target, CALIB, rerun, visit)

        if 'target' in cmdKeys or 'rerun' in cmdKeys:
            butler = dafPersist.Butler(os.path.join(target, 'rerun', rerun))
        else:
            butler = self.engine.butler

        imPath = butler.getUri('calexp', arm=arm, visit=visit)
        cmd.finish(f"detrend={imPath}")

    def startDotRoach(self, cmd):
        """ Start dot loop. """
        cmdKeys = cmd.cmd.keywords
        # output root.
        dataRoot = cmdKeys['dataRoot'].values[0]
        # which fiber/cobra to put behind dot.
        maskFile = cmdKeys['maskFile'].values[0]
        # keep moving no matter what.
        keepMoving = 'keepMoving' in cmdKeys

        self.engine.startDotRoach(dataRoot=dataRoot, maskFile=maskFile, keepMoving=keepMoving)
        cmd.finish('text="starting loop... Run dotRoaches, run ! "')

    def processDotRoach(self, cmd):
        """ Data is actually processed on the fly, just basically generate status. """
        self.engine.dotRoach.status(cmd)
        cmd.finish()

    def stopDotRoach(self, cmd):
        """ Stop dot loop. """
        self.engine.stopDotRoach(cmd)
        cmd.finish()

    def checkLeftOvers(self, cmd):
        """ Check for non-reduced files."""
        self.engine.checkLeftOvers(cmd)
        cmd.finish(f'text="len(fileBuffer)={len(self.engine.fileBuffer)}"')
