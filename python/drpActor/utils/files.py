from ics.utils.sps.spectroIds import SpectroIds
import os


class CCDFile(object):
    """ just a place holder for ccd file"""
    fromArmNum = dict([(v, k) for k, v in SpectroIds.validArms.items()])

    def __init__(self, root, night, filename):
        self.root = root
        self.night = night
        self.filename = filename
        self.visit = int(filename[4:10])
        self.specNum = int(filename[10])
        self.armNum = int(filename[11])
        self.arm = CCDFile.fromArmNum[self.armNum]

        self.ingested = False
        self.detrended = False

    @property
    def filepath(self):
        return os.path.join(self.root, self.night, 'sps', self.filename)

    @property
    def starPath(self):
        return os.path.join(self.root, self.night, 'sps', f'{self.filename[:10]}*.fits')
