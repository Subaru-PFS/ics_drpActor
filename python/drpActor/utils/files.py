import os

import fysom
from ics.utils.sps.spectroIds import SpectroIds


class PfsFile(fysom.Fysom):
    """ just a place holder for ccd file"""
    fromArmNum = dict([(v, k) for k, v in SpectroIds.validArms.items()])

    def __init__(self, root, night, filename):
        fysom.Fysom.__init__(self, {'initial': 'none',
                                    'events': [{'name': 'ingest', 'src': 'none', 'dst': 'ingesting'},
                                               {'name': 'accept', 'src': ['none', 'ingesting'], 'dst': 'ingested'},
                                               {'name': 'reject', 'src': ['none', 'ingesting'], 'dst': 'none'},
                                               {'name': 'reduce', 'src': ['ingested'], 'dst': 'reducing'},
                                               {'name': 'idle', 'src': ['ingested', 'reducing'], 'dst': 'ingested'},
                                               ]})
        self.root = root
        self.night = night
        self.filename = filename
        self.visit = int(filename[4:10])
        self.specNum = int(filename[10])
        self.armNum = int(filename[11])
        self.arm = PfsFile.fromArmNum[self.armNum]

        self.calexp = ''
        self.pfsArm = False

    @property
    def unknown(self):
        return self.current == 'none'

    @property
    def ingested(self):
        return self.current in ['ingested', 'reducing']

    @property
    def reducible(self):
        return self.current == 'ingested'

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, self.filename)

    @property
    def dataId(self):
        return dict(visit=self.visit, arm=self.arm, spectrograph=self.specNum)

    def isIngested(self, butler):
        """Check is file has been ingested, setting state machine."""
        if not self.ingested:
            try:
                butler.get('raw_md', **self.dataId)
                self.accept()
            except:
                self.reject()

        return self.ingested

    def getCalexp(self, butler):
        """Check is calexp has been produced."""
        if not self.calexp and self.isIngested(butler):
            try:
                self.calexp = butler.getUri('calexp', **self.dataId)
            except:
                pass

        return self.calexp

    def getPfsArm(self, butler):
        """Check if armFile has been produced."""
        if not self.pfsArm and self.isIngested(butler):
            try:
                self.pfsArm = butler.get('pfsArm', **self.dataId)
            except:
                pass

        return self.pfsArm


class CCDFile(PfsFile):

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, 'sps', self.filename)


class HxFile(PfsFile):
    pass
