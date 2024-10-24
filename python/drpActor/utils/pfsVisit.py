import logging

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

    def getCalexp(self, butler):
        """
        Retrieve the file paths of calibrated exposures (calexp) from the Butler datastore.

        Parameters
        ----------
        butler : lsst.daf.butler.Butler
            The Butler instance used to query the datastore.

        Returns
        -------
        list of str
            A list of file paths for the calibrated exposures. If a calexp is not found
            for a specific data ID, a warning is logged and it is skipped.
        """

        # Initialize logger
        logger = logging.getLogger('postReduction')

        # List to store the file paths of found calibrated exposures
        filepaths = []

        # Iterate over the exposure files to retrieve their URIs from the datastore
        for file in self.exposureFiles:
            try:
                # Get the URI for the 'calexp' dataset using the file's data ID
                uri = butler.getURI('calexp', **file.dataId)

                # Extract the file path from the URI (assuming 'file://' prefix)
                filepaths.append(uri.split('file://')[1])
            except Exception as e:
                # Log a warning if the calexp is not found for a specific data ID
                logger.warning(f"Could not find calexp for {file.dataId}")

        # Return the list of found file paths
        return filepaths

    def addExposure(self, exposureFile):
        """Add an exposure file to the list of exposures for the visit."""
        self.exposureFiles.append(exposureFile)
