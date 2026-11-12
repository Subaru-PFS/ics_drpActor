"""Follow the outputs a reduction writes into its run while it is running, to announce them."""

import time

from lsst.daf.butler import Butler, MissingCollectionError, MissingDatasetTypeError


def detrendKey(dataId, filepath, run, elapsed):
    """detrend keyword: the file of a detrended image."""
    return f'detrend={filepath}'


def pfsArmStatusKey(dataId, filepath, run, elapsed):
    """pfsArmStatus keyword: visit, arm, spectrograph, run and seconds since the reduction started."""
    return f'pfsArmStatus={dataId["visit"]},"{dataId["arm"]}",{dataId["spectrograph"]},"{run}",{elapsed:.1f}'


KEYWORDS = dict(detrend=detrendKey, pfsArmStatus=pfsArmStatusKey)


class OutputWatcher:
    """
    Report the datasets of one type written into a run, each dataset once.

    The datasets already in the run at creation are not reported, a dataset replaced by a later write is, as a new
    dataset. A failing butler query is logged once and stops the reports, it never raises.

    Parameters
    ----------
    datastore : str
        Butler repo root/URI.
    run : str
        RUN collection receiving the reduction's outputs, which may not exist yet.
    where : str
        Data query of the reduction, also selecting the datasets reported.
    datasetType : str
        Dataset type reported, e.g. 'postISRCCD' or 'pfsArm'.
    logger : logging.Logger
        Logger receiving the failure.
    """

    def __init__(self, datastore, run, where, datasetType, logger):
        self.run = run
        self.where = where
        self.datasetType = datasetType
        self.logger = logger
        self.start = time.monotonic()
        self.seen = set()
        self.butler = None

        try:
            self.butler = Butler(datastore)
            self.seen = {ref.id for ref in self.written()}
        except Exception as e:
            self.disable(e)

    def disable(self, error):
        """Stop the reports, logging why."""
        self.logger.warning(f'{self.datasetType} announcements disabled for where="{self.where}": {error}')
        self.butler = None

    def elapsed(self):
        """Seconds since the watcher was created."""
        return time.monotonic() - self.start

    def written(self):
        """
        Datasets currently in the run.

        Returns
        -------
        refs : list of lsst.daf.butler.DatasetRef
            Empty while the run or the dataset type does not exist.
        """
        try:
            return list(self.butler.registry.queryDatasets(self.datasetType, collections=self.run,
                                                           where=self.where, instrument='PFS'))
        except MissingCollectionError:
            # the run is created by the reduction, the registry caches the collections it knows.
            self.butler.registry.refresh()
            return []
        except MissingDatasetTypeError:
            return []

    def poll(self):
        """
        Datasets written since the last poll.

        Returns
        -------
        outputs : list of tuple
            (dataId, filepath) sorted by data ID, dataId a dict of the dataset's required dimensions, empty once the
            reports are stopped.
        """
        if self.butler is None:
            return []

        try:
            new = [ref for ref in self.written() if ref.id not in self.seen]
            outputs = [(dict(ref.dataId.required), self.butler.getURI(ref).ospath) for ref in new]
        except Exception as e:
            self.disable(e)
            return []

        self.seen.update(ref.id for ref in new)

        return sorted(outputs, key=lambda output: sorted(output[0].items()))
