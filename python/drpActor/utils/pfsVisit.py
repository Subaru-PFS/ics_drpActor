from drpActor.utils.files import PfsConfigFile


class PfsVisit(object):
    """
    Represents a PFS visit, containing associated exposures and pfsConfig information.

    Parameters
    ----------
    visit : int
        The visit identifier.
    pfsConfigFile : str, optional
        Path to the corresponding pfsConfig file, by default None.
    """

    def __init__(self, visit, pfsConfigFile=None):
        self.visit = visit

        # just instantiate with no filepath
        if pfsConfigFile is None:
            pfsConfigFile = PfsConfigFile(visit, filepath=None)

        self.pfsConfigFile = pfsConfigFile

        self.exposureFiles = []

    @property
    def allFiles(self):
        return self.exposureFiles + [self.pfsConfigFile]

    @property
    def isIngested(self):
        return all([file.ingested for file in self.allFiles])

    def addExposure(self, exposureFile):
        """Add an exposure file to the list of exposures for the visit."""
        self.exposureFiles.append(exposureFile)
