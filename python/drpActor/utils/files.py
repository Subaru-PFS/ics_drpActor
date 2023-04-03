import os

from ics.utils.sps.spectroIds import SpectroIds


class PfsFile(object):
    """ just a place holder for ccd file"""
    fromArmNum = dict([(v, k) for k, v in SpectroIds.validArms.items()])

    def __init__(self, root, night, filename):

        self.root = root
        self.night = night
        self.filename = filename
        self.visit = int(filename[4:10])
        self.specNum = int(filename[10])
        self.armNum = int(filename[11])
        self.arm = PfsFile.fromArmNum[self.armNum]

        self.raw_md = None
        self.calexp = None
        self.pfsArm = None

        self.state = 'idle'

    @property
    def ingested(self):
        return self.raw_md is not None

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, self.filename)

    @property
    def dataId(self):
        return dict(visit=self.visit, arm=self.arm, spectrograph=self.specNum)

    def initialize(self, butler):
        """Set file to correct state."""
        self.getRawMd(butler)
        self.getCalexp(butler)
        self.getPfsArm(butler)

    def getRawMd(self, butler):
        """Check is file has been ingested, setting state machine."""
        if not self.raw_md:
            try:
                self.raw_md = butler.get('raw_md', **self.dataId)
            except:
                pass

        return self.raw_md

    def getCalexp(self, butler):
        """Check is calexp has been produced."""
        if self.ingested and not self.calexp:
            try:
                self.calexp = butler.getUri('calexp', **self.dataId)
            except Exception as e:
                print(e)

        return self.calexp

    def getPfsArm(self, butler):
        """Check if armFile has been produced."""
        if self.ingested and not self.pfsArm:
            try:
                self.pfsArm = butler.get('pfsArm', **self.dataId)
            except:
                pass

        return self.pfsArm


class CCDFile(PfsFile):

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, 'sps', self.filename)

    @property
    def windowed(self):
        try:
            nRows = self.raw_md['W_CDROWN'] + 1 - self.raw_md['W_CDROW0']
            return nRows != 4300
        except KeyError:
            return False


class HxFile(PfsFile):
    @property
    def windowed(self):
        return False
