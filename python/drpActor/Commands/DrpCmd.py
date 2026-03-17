import glob
import os
from importlib import reload

import drpActor.utils.drpParsing as drpParsing
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.utils.files import CCDFile, HxFile, PfsConfigFile


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

            ('ingest', '<visit> [<spectrograph>] [<arm>] [@(newEngine)]', self.ingest),
            ('reduce', '<where> [@(skipRequireAdjustDetectorMap)] [@(quickCDS)]', self.reduce),

            ('startDotRoach', '<cams>', self.startDotRoach),
            ('stopDotRoach', '', self.stopDotRoach),
            ('processDotRoach', '', self.processDotRoach),

        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("rerun", types.String(), help="rerun drp folder"),
                                        keys.Key("filepath", types.String(), help="Raw FITS File path"),
                                        keys.Key("visit", types.String(),
                                                 help="visit argument, same parsing as 2d drp pipeline "
                                                      "eg visit=1..10 or visit=101^102"),
                                        keys.Key("spectrograph", types.String(),
                                                 help="optional spectrograph argument, same parsing as 2d drp pipeline "
                                                      "eg spectrograph=1^4"),
                                        keys.Key("arm", types.String(),
                                                 help="optional arm argument, same parsing as 2d drp pipeline "
                                                      "eg arm=b^r"),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera used for roaching.'),

                                        keys.Key("where", types.String(), help="where condition "
                                                                               "to select the dataset to reduce"),
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

    def getEngine(self, cmdKeys):
        """Get drp engine."""
        engine = self.actor.engine if 'newEngine' not in cmdKeys else self.actor.loadDrpEngine()
        return engine

    def ingest(self, cmd):
        """Ingest visits into the configured datastore."""
        cmdKeys = cmd.cmd.keywords

        visits = cmdKeys["visit"].values[0]
        spectrograph = cmdKeys["spectrograph"].values[0] if 'spectrograph' in cmdKeys else None
        arms = cmdKeys["arm"].values[0] if 'arm' in cmdKeys else None

        engine = self.getEngine(cmdKeys)

        visitList = drpParsing.makeVisitList(visits)

        for visit in visitList:
            try:
                pfsConfigPath, pattern = drpParsing.makeVisitPattern(visit, spectrograph=spectrograph, arms=arms)
            except RuntimeError:
                cmd.warn(f'text="{visit} not found..."')
                continue

            pfsConfigFile = PfsConfigFile(visit, filepath=pfsConfigPath)
            engine.newPfsConfig(pfsConfigFile)

            for filepath in glob.glob(pattern):
                rootNightType, fname = os.path.split(filepath)
                rootNight, fitsType = os.path.split(rootNightType)
                root, night = os.path.split(rootNight)
                File = HxFile if fitsType == 'ramps' else CCDFile
                engine.newExposure(File(root, night, fname))

            pfsVisit = engine.pfsVisits.get(visit)
            engine.ingestHandler.doIngest(pfsVisit, cmd=cmd)

        cmd.finish()

    def reduce(self, cmd):
        """Ingest file providing a filepath."""
        cmdKeys = cmd.cmd.keywords

        where = cmdKeys["where"].values[0]

        # Allow users to write string literals with double quotes inside the actor-quoted where:
        # where="'arm in (\"b\", \"r\")'"
        # Butler requires single-quoted strings, so convert "..." -> '...'.
        if '"' in where:
            where = where.replace('"', "'")

        requireAdjustDetectorMap = 'skipRequireAdjustDetectorMap' not in cmdKeys
        quickCDS = 'quickCDS' in cmdKeys

        engine = self.getEngine(cmdKeys)
        configOverride = dict(reduceExposure={'requireAdjustDetectorMap': requireAdjustDetectorMap},
                              isr={'h4.quickCDS': quickCDS})
        engine.addConfigOverride(configOverride)
        engine.runReductionPipeline(where=where)

        cmd.finish()

    def startDotRoach(self, cmd):
        """Start dot roach loop."""
        cmdKeys = cmd.cmd.keywords
        cams = cmdKeys['cams'].values
        self.engine.startDotRoach(cams)
        cmd.finish('text="starting loop... Run dotRoaches, run ! "')

    def processDotRoach(self, cmd):
        """Wait for flux extraction results and report status."""
        self.engine.dotRoach.waitForResult()
        self.engine.dotRoach.status(cmd)
        cmd.finish()

    def stopDotRoach(self, cmd):
        """Stop dot roach loop."""
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
