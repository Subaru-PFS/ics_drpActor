from drpActor.utils.files import PfsConfigFile


class PfsVisit:
    """
    Represents a PFS visit, containing associated exposures and `pfsConfig` information.

    Parameters
    ----------
    visit : int
        The visit identifier for this observation.
    pfsConfigFile : PfsConfigFile, optional
        A `PfsConfigFile` object representing the configuration file for the visit.
        If not provided, a new `PfsConfigFile` instance will be created with `filepath=None`.

    Attributes
    ----------
    visit : int
        The visit ID for this observation.
    pfsConfigFile : PfsConfigFile
        The configuration file associated with the visit.
    exposureFiles : list
        A list of exposure files associated with the visit.
    wasProcessed : bool
        Indicates whether the visit has been processed.
    """

    def __init__(self, visit, pfsConfigFile=None):
        """
        Initialize the PfsVisit instance.

        Parameters
        ----------
        visit : int
            The visit ID for this observation.
        pfsConfigFile : PfsConfigFile, optional
            The configuration file associated with the visit.
            If not provided, a new instance is created with `filepath=None`.
        """
        self.visit = visit
        self.wasProcessed = False  # Track whether the visit was processed.

        # Instantiate pfsConfigFile if not provided
        if pfsConfigFile is None:
            pfsConfigFile = PfsConfigFile(visit, filepath=None)

        self.pfsConfigFile = pfsConfigFile
        self.exposureFiles = []  # Initialize the list of exposure files.

    @property
    def allFiles(self):
        """
        Get a list of all files associated with the visit, including exposures and pfsConfig.

        Returns
        -------
        list
            A list of all files for the visit, including the pfsConfig file.
        """
        return self.exposureFiles + [self.pfsConfigFile]

    @property
    def isIngested(self):
        """
        Check if all files associated with the visit have been ingested into the datastore.

        Returns
        -------
        bool
            True if all files are ingested, False otherwise.
        """
        return all(file.ingested for file in self.allFiles)

    def addExposure(self, exposureFile):
        """
        Add an exposure file to the list of exposures for the visit.

        Parameters
        ----------
        exposureFile : PfsFile
            The exposure file to add to the visit.
        """
        self.exposureFiles.append(exposureFile)

    def setupDetrendCallback(self, engine):
        """
        Set up a callback to monitor the completion of post-ISR processing for exposures.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            The Butler instance used to query the datastore.
        callback : callable
            A callback function to invoke when the post-ISR image is available.

        Notes
        -----
        This method iterates over all exposure files associated with the visit and
        sets up a callback for each one to notify when the post-ISR image becomes available.
        """
        for exposureFile in self.exposureFiles:
            exposureFile.setupDetrendKeyCallback(engine)

    def finish(self):
        """Placeholder"""
        self.wasProcessed = True
