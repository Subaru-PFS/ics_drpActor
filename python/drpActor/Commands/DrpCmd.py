import glob
import os
import time
from importlib import reload

import drpActor.utils.dotRoach as dotRoach
import drpActor.utils.drpParsing as drpParsing
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.utils.files import CCDFile, HxFile

reload(dotRoach)


class DrpCmd(object):
    reduceTimeout = 300

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
            ('reduce', '<visit> [<spectrograph>] [<arm>]', self.reduce),

            ('startDotRoach', '<dataRoot> <maskFile> <cams> [@(keepMoving)]', self.startDotRoach),
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
                                        keys.Key("iteration", types.Int(), help="dotRoach iteration"),
                                        keys.Key("visit", types.String(),
                                                 help="visit argument, same parsing as 2d drp pipeline "
                                                      "eg visit=1..10 or visit=101^102"),
                                        keys.Key("spectrograph", types.String(),
                                                 help="optional spectrograph argument, same parsing as 2d drp pipeline "
                                                      "eg spectrograph=1^4"),
                                        keys.Key("arm", types.String(),
                                                 help="optional arm argument, same parsing as 2d drp pipeline "
                                                      "eg arm=b^r"),
                                        keys.Key("dataRoot", types.String(),
                                                 help="dataRoot which will contain the generated outputs"),
                                        keys.Key("maskFile", types.String(),
                                                 help="config file describing which cobras will be put behind dots."),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera used for roaching.'),
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

    def reduce(self, cmd):
        """Ingest file providing a filepath."""
        cmdKeys = cmd.cmd.keywords

        visits = cmdKeys["visit"].values[0]
        spectrograph = cmdKeys["spectrograph"].values[0] if 'spectrograph' in cmdKeys else None
        arms = cmdKeys["arm"].values[0] if 'arm' in cmdKeys else None

        engine = self.actor.loadDrpEngine()

        visitList = drpParsing.makeVisitList(visits)

        for visit in visitList:
            pattern = drpParsing.makeVisitPattern(visit, spectrograph=spectrograph, arms=arms)
            fitsFiles = glob.glob(pattern)
            start = time.time()
            for filepath in fitsFiles:
                cmd.inform(f'text="Processing {filepath}"')
                rootNightType, fname = os.path.split(filepath)
                rootNight, fitsType = os.path.split(rootNightType)
                root, night = os.path.split(rootNight)
                file = HxFile(rootNight, fitsType, fname) if fitsType == 'ramps' else CCDFile(root, night, fname)
                engine.newExposure(file, doCheckPfsConfigFlag=False)

            time.sleep(2)

            sameVisit = [file for file in engine.fileBuffer if file.visit == visit]

            while not all([file.state == 'idle' for file in sameVisit]):
                time.sleep(1)

                if time.time() - start > DrpCmd.reduceTimeout:
                    raise RuntimeError(f'failed to reduce visits in {DrpCmd.reduceTimeout} seconds')

            engine.genIngestStatus(visit, cmd=cmd)
            for file in sameVisit:
                engine.inspect(file)
                file.genAllKeys(cmd)

        engine.fileBuffer = []
        engine.butler = None
        engine.tasks = None
        del engine

        cmd.finish()

    def startDotRoach(self, cmd):
        """ Start dot loop. """
        cmdKeys = cmd.cmd.keywords
        # output root.
        dataRoot = cmdKeys['dataRoot'].values[0]
        # which fiber/cobra to put behind dot.
        maskFile = cmdKeys['maskFile'].values[0]
        # keep moving no matter what.
        keepMoving = 'keepMoving' in cmdKeys
        # which camera are used for roaching.
        cams = cmdKeys['cams'].values

        # lookup for any un-finalized roaching and finish it.
        if os.path.isdir(dataRoot):
            cmd.inform('text="found a DotRoach left-over, finishing it now..."')
            roach = dotRoach.DotRoach(self.engine, dataRoot, maskFile, "")
            roach.finish()

        self.engine.startDotRoach(dataRoot, maskFile, cams, keepMoving=keepMoving)
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
