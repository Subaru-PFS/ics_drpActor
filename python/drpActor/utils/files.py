import glob
import os
import time

from drpActor.utils.threading import singleShot
from ics.utils.sps.spectroIds import SpectroIds


class PfsConfigFile:
    """
    Represents a PFS configuration file for a specific visit.

    Parameters
    ----------
    visit : int
        The visit identifier associated with the configuration file.
    filepath : str, optional
        The path to the pfsConfig file, if available.

    Attributes
    ----------
    visit : int
        Visit identifier.
    filepath : str or None
        File path of the pfsConfig file.
    ingested : bool
        Indicates whether the file has been ingested into the datastore.
    """

    fileNameFormat = "pfsConfig-0x%016x-%06d.fits"
    rootDir = '/data/raw'

    def __init__(self, visit, filepath=None):
        self.visit = visit
        self.filepath = filepath
        self.ingested = False

    @property
    def dataId(self):
        """Return the data ID dictionary used by the Butler."""
        return dict(visit=self.visit)

    @classmethod
    def fromKeys(cls, pfsDesignId, visit, dateDir):
        """Create a PfsConfigFile instance from design ID, visit, and date directory."""
        filepath = os.path.join(cls.rootDir, dateDir, 'pfsConfig', cls.fileNameFormat % (pfsDesignId, visit))
        return cls(visit, filepath=filepath)

    def initialize(self, butler):
        """
        Set the state of the file based on its presence in the datastore.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            Butler instance to query the datastore.

        Notes
        -----
        Sets `self.ingested` to True if the pfsConfig file is found in the datastore.
        """
        self.ingested = len(list(butler.registry.queryDatasets("pfsConfig", **self.dataId))) == 1


class PfsFile:
    """
    Placeholder class representing a raw Exposure file CCD or H4.

    Parameters
    ----------
    root : str
        The root directory of the data files.
    night : str
        The observing night associated with the file.
    filename : str
        The name of the raw data file.

    Attributes
    ----------
    visit : int
        The visit ID extracted from the filename.
    specNum : int
        The spectrograph number extracted from the filename.
    armNum : int
        The arm number (channel) extracted from the filename.
    arm : str
        The arm/channel identifier (e.g., 'b', 'r', 'm').
    ingested : bool
        Indicates whether the file has been ingested into the datastore.
    """

    fromArmNum = dict([(v, k) for k, v in SpectroIds.validArms.items()])

    def __init__(self, root, night, filename):
        """Initialize the PfsFile object."""
        self.root = root
        self.night = night
        self.filename = filename

        # Extract metadata from filename
        self.visit = PfsFile.toVisit(filename)
        self.specNum = PfsFile.toSpecNum(filename)
        self.armNum = PfsFile.toArmNum(filename)

        self.arm = PfsFile.fromArmNum[self.armNum]
        self.ingested = False
        self.postIsrFilepath = ''

    @property
    def filepath(self):
        """Return the full path to the file."""
        return os.path.join(self.root, self.night, self.filename)

    @property
    def dataId(self):
        """Return the data ID dictionary used by the Butler."""
        return dict(visit=self.visit, arm=self.arm, spectrograph=self.specNum)

    @property
    def cam(self):
        """Return the camera identifier (e.g., 'r1', 'b2')."""
        arm = 'r' if self.arm in 'rm' else self.arm
        return f'{arm}{self.specNum}'

    @staticmethod
    def toVisit(filename):
        """Extract the visit ID from the filename."""
        return int(filename[4:10])

    @staticmethod
    def toSpecNum(filename):
        """Extract the spectrograph number from the filename."""
        return int(filename[10])

    @staticmethod
    def toArmNum(filename):
        """Extract the arm number from the filename."""
        return int(filename[11])

    def initialize(self, butler):
        """
        Set the ingestion state of the file based on its presence in the datastore.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            Butler instance to query the datastore.

        Notes
        -----
        Sets `self.ingested` to True if the raw file is found in the datastore.
        """
        self.ingested = len(list(butler.registry.queryDatasets("raw", **self.dataId))) == 1

    def getPostIsrFilepath(self, engine):
        """
        Retrieve the file path of the post-ISR image.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            Butler instance used to query the datastore.

        Returns
        -------
        str
            The local file path to the post-ISR image.
        """

        def toPostISRCCDFilename(dataId, outputCollection, run):
            parts = ['postISRCCD', 'PFS', f'{dataId["visit"]:06d}', f'{dataId["arm"]}{dataId["spectrograph"]}',
                     outputCollection.replace('/', '_'), run]
            filename = f'{"_".join(parts)}.fits'
            return filename

        directory = os.path.join(engine.datastore, engine.outputCollection, engine.timestamp, 'postISRCCD',
                                 self.night.replace('-', ''), f'{self.dataId["visit"]:06d}')
        filename = toPostISRCCDFilename(self.dataId, engine.outputCollection, engine.timestamp)
        fullPath = os.path.join(directory, filename)

        return fullPath

    @singleShot
    def setupDetrendKeyCallback(self, engine):
        """
        Monitor the datastore for the presence of post-ISR images and trigger a callback.

        Parameters
        ----------
        pfsVisit : int
            The visit ID associated with the observation.
        butler : lsst.daf.butler.Butler
            Butler instance used to query the datastore.
        callback : callable
            The callback function to invoke when the post-ISR image is available.

        Notes
        -----
        This function waits for the post-ISR image to become available within a time limit.
        If the image is found, the callback is triggered with the image's file path.
        If not found, a warning is logged.
        """
        # constructing the path only once.
        self.postIsrFilepath = self.getPostIsrFilepath(engine)

        def postIsrWasGenerated():
            """Check if the post-ISR image is available in the datastore."""
            if not self.postIsrFilepath:
                return False
            return len(glob.glob(self.postIsrFilepath)) != 0

        start_time = time.time()
        config = engine.actor.actorConfig['genDetrendKey']

        # Wait for the post-ISR image to become available or until timeout
        while not postIsrWasGenerated():
            if (time.time() - start_time) > config['timeout']:
                break

            time.sleep(config['waitInterval'])

        # If post-ISR image found, trigger callback; otherwise, log a warning
        if postIsrWasGenerated():
            engine.actor.bcast.inform(f'detrend={self.postIsrFilepath}')


class CCDFile(PfsFile):
    """
    Specialized class representing a CCD file from the PFS instrument.

    Inherits from `PfsFile` and adds specific attributes related to CCD data.

    Attributes
    ----------
    filepath : str
        Overrides the parent `filepath` to point to the correct subdirectory.
    windowed : bool
        Indicates whether the CCD data is windowed based on metadata.
    """

    @property
    def filepath(self):
        """Return the full path to the CCD file within the 'sps' directory."""
        return os.path.join(self.root, self.night, 'sps', self.filename)

    @property
    def windowed(self):
        """
        Determine if the CCD data is windowed.

        Returns
        -------
        bool
            True if the data is windowed, False otherwise.
        """
        try:
            nRows = self.raw_md['W_CDROWN'] + 1 - self.raw_md['W_CDROW0']
            return nRows != 4300
        except KeyError:
            return False


class HxFile(PfsFile):
    """
    Specialized class representing h4 file from the PFS instrument.

    Inherits from `PfsFile` and provides specific attributes for Hx data.
    """

    @property
    def windowed(self):
        """
        Determine if the Hx data is windowed.

        Returns
        -------
        bool
            Always returns False since Hx data is not windowed.
        """
        return False
