import os
import time
from importlib import reload

import drpActor.utils.dotRoach as dotRoach
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.utils.files import CCDFile, HxFile
from ics.utils.sps.spectroIds import SpectroIds

reload(dotRoach)


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
            ('detrend', '<visit> <arm> <spectrograph>', self.detrend),

            ('startDotRoach', '<dataRoot> <maskFile> [@(keepMoving)]', self.startDotRoach),
            ('stopDotRoach', '', self.stopDotRoach),
            ('processDotRoach', '<iteration>', self.processDotRoach),
            ('dotRoach', '@phase2', self.dotRoachPhase2),
            ('dotRoach', '@phase3', self.dotRoachPhase3),

            ('doReduce', '[@(off|on)]', self.doReduce),
            ('doDetrend', '[@(off|on)]', self.doDetrend),

        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("rerun", types.String(), help="rerun drp folder"),
                                        keys.Key("filepath", types.String(), help="Raw FITS File path"),
                                        keys.Key("arm", types.String(), help="arm"),
                                        keys.Key("iteration", types.Int(), help="dotRoach iteration"),
                                        keys.Key("visit", types.Int(), help="visitId"),
                                        keys.Key("spectrograph", types.Int(), help="spectrograph id"),
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
        self.actor.sendVersionKey(cmd)

        cmd.inform('text="Present!"')
        cmd.finish()

    def ingest(self, cmd):
        """Ingest file providing a filepath."""
        cmdKeys = cmd.cmd.keywords

        filepath = cmdKeys["filepath"].values[0]
        rootNight, fname = os.path.split(filepath)

        if 'sps' in filepath:
            File = CCDFile
            rootNight, __ = os.path.split(rootNight)
        else:
            File = HxFile

        root, night = os.path.split(rootNight)

        file = File(root, night, fname)
        self.engine.inspect(file)

        if file.ingested:
            cmd.finish('text="file already ingested..."')
            return

        self.engine.tasks.ingest(file)

        cmd.finish(f"ingest={filepath}")

    def detrend(self, cmd):
        """Detrend image providing dataId"""
        cmdKeys = cmd.cmd.keywords

        visit = cmdKeys["visit"].values[0]
        spectrograph = cmdKeys["spectrograph"].values[0]
        arm = cmdKeys["arm"].values[0]

        File = HxFile if arm == 'n' else CCDFile

        fname = f'PF**{visit:06d}{spectrograph}{SpectroIds.validArms[arm]}.fits'
        root = '/data/raw'
        night = '2023-*-*/'

        file = File(root, night, fname)
        self.engine.inspect(file)

        if not file.ingested:
            cmd.fail('text="file was not ingested..."')
            return

        self.engine.tasks.detrend(file)

        start = time.time()

        while file.state != 'idle' and file.calexp is None:
            if (time.time() - start) > 180:
                raise RuntimeError('could not get detrend image after 180 secs')
            time.sleep(1)

        cmd.finish(f"detrend={file.calexp}")

    def startDotRoach(self, cmd):
        """ Start dot loop. """
        cmdKeys = cmd.cmd.keywords
        # output root.
        dataRoot = cmdKeys['dataRoot'].values[0]
        # which fiber/cobra to put behind dot.
        maskFile = cmdKeys['maskFile'].values[0]
        # keep moving no matter what.
        keepMoving = 'keepMoving' in cmdKeys

        # lookup for any un-finalized roaching and finish it.
        if os.path.isdir(dataRoot):
            cmd.inform('text="found a DotRoach left-over, finishing it now..."')
            roach = dotRoach.DotRoach(self.engine, dataRoot, maskFile)
            roach.finish()

        self.engine.startDotRoach(dataRoot=dataRoot, maskFile=maskFile, keepMoving=keepMoving)
        cmd.finish('text="starting loop... Run dotRoaches, run ! "')

    def processDotRoach(self, cmd):
        """ Data is actually processed on the fly, just basically generate status. """
        cmdKeys = cmd.cmd.keywords
        iteration = cmdKeys['iteration'].values[0]

        self.engine.dotRoach.waitForResult(iteration)
        self.engine.dotRoach.status(cmd)

        cmd.finish()

    def dotRoachPhase2(self, cmd):
        """ Data is actually processed on the fly, just basically generate status. """
        self.engine.dotRoach.phase2()
        cmd.finish()

    def dotRoachPhase3(self, cmd):
        """ Data is actually processed on the fly, just basically generate status. """
        self.engine.dotRoach.phase3()
        cmd.finish()

    def stopDotRoach(self, cmd):
        """ Stop dot loop. """
        self.engine.stopDotRoach(cmd)
        cmd.finish()

    def doDetrend(self, cmd):
        """activating/deactivating auto reduction."""
        cmdKeys = cmd.cmd.keywords

        doDetrend = 'off' not in cmdKeys
        self.engine.doAutoDetrend = doDetrend
        cmd.finish(f'text="do drp.doAutoDetrend {doDetrend}"')

    def doReduce(self, cmd):
        """activating/deactivating auto reduction."""
        cmdKeys = cmd.cmd.keywords

        doReduce = 'off' not in cmdKeys
        self.engine.doAutoReduce = doReduce
        cmd.finish(f'text="do drp.autoReduce {doReduce}"')
