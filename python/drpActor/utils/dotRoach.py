import logging
import multiprocessing
import time

import drpActor.utils.extractFlux as extractFlux
import pandas as pd
from pfs.datamodel.pfsConfig import FiberStatus
from pfs.utils.fiberids import FiberIds


class DotRoach(object):
    processTimeout = 25 + 120

    def __init__(self, engine, cams):
        """Placeholder to handle DotRoach loop."""
        self.engine = engine
        self.processManager = multiprocessing.Manager()
        self.cams = cams

        self.normFactor = None
        self.currentVisit = None
        self.pfsConfig = None
        self.detectorMaps = dict()
        self.fiberTraces = dict()

    @property
    def monitoringFiberIds(self):
        # BROKENCOBRA fibers are parked at home — use them as lamp monitors.
        atHome = self.pfsConfig[self.pfsConfig.fiberStatus == FiberStatus.BROKENCOBRA].fiberId
        return list(atHome)

    def run(self, pfsVisit):
        """Run method using pfsVisit."""
        relevantFiles = [f for f in pfsVisit.exposureFiles if f.cam in self.cams]
        if not relevantFiles:
            return
        self.runAway(pfsVisit.visit, relevantFiles)

    def collectFiberData(self, files):
        """Retrieve raw exposures from butler and return flux per fiber."""
        fluxPerFiber = self.processManager.list()
        jobs = []

        def parallelize(exp, dataId, fiberTrace, detectorMap, fluxPerFiber):
            flux = extractFlux.getWindowedFluxes(exp, dataId, fiberTrace=fiberTrace, detectorMap=detectorMap)
            fluxPerFiber.append(flux)

        # Load fiberTraces on first iteration.
        if not self.fiberTraces:
            self.pfsConfig = self.engine.butler.get('pfsConfig', files[0].dataId)
            for file in files:
                self.getFiberTrace(file.dataId)

        for file in files:
            fiberTrace, detectorMap = self.getFiberTrace(file.dataId)
            exp = self.engine.butler.get('raw.exposure', file.dataId)
            p = multiprocessing.Process(target=parallelize,
                                        args=(exp.convertF(), file.dataId, fiberTrace, detectorMap, fluxPerFiber))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        return pd.concat(fluxPerFiber).groupby('fiberId').sum().reset_index()

    def getFiberTrace(self, dataId):
        """Retrieve fiberTrace, cached per camera."""
        cameraKey = dataId['spectrograph'], dataId['arm']

        if cameraKey not in self.fiberTraces:
            logging.info(f'making fiberTrace for {cameraKey}')
            fiberProfiles = self.engine.butler.get('fiberProfiles', dataId)
            # detectorMap = self.engine.butler.get('detectorMap_calib', dataId)
            # I want to use an adjusted detectorMap instead.
            detectorMap = self.engine.butler.get('detectorMap', dataId)
            self.detectorMaps[cameraKey] = detectorMap
            self.fiberTraces[cameraKey] = fiberProfiles.makeFiberTracesFromDetectorMap(detectorMap)

        return self.fiberTraces[cameraKey], self.detectorMaps[cameraKey]

    def fluxNormalized(self, fluxPerFiber):
        """Return scalar normalization factor correcting for lamp response drift."""
        lampResponse = fluxPerFiber.set_index('fiberId').reindex(self.monitoringFiberIds).dropna().flux.sum()
        lampResponse = float(lampResponse) if lampResponse else 1.0

        if self.normFactor is None:
            self.normFactor = lampResponse

        return self.normFactor / lampResponse

    def runAway(self, visit, files):
        """Extract flux, normalize, and bulk-insert into opdb."""
        self.currentVisit = visit

        fluxPerFiber = self.collectFiberData(files)
        normFactor = self.fluxNormalized(fluxPerFiber)

        # Drop engineering fibers — only cobras go to the DB.
        fluxPerCobra = fluxPerFiber[fluxPerFiber.cobraId != FiberIds.MISSING_VALUE].copy()
        fluxPerCobra['flux_norm'] = fluxPerCobra.flux.to_numpy() * normFactor
        fluxPerCobra['pfs_visit_id'] = visit
        fluxPerCobra = fluxPerCobra.rename(columns={'cobraId': 'cobra_id'})[
            ['pfs_visit_id', 'cobra_id', 'flux', 'flux_norm']]

        self.engine.opdb.insert('dot_roach_flux', fluxPerCobra)

    def waitForResult(self):
        """Wait until dot_roach_flux rows for the current visit appear in opdb."""
        start = time.time()

        while True:
            count = self.engine.opdb.query_scalar(
                'SELECT COUNT(*) FROM dot_roach_flux WHERE pfs_visit_id = :visit_id',
                params={'visit_id': self.currentVisit},
            )
            if count:
                return

            now = time.time()
            if (now - start) > DotRoach.processTimeout:
                raise RuntimeError(
                    f'no dot_roach_flux results for visit={self.currentVisit} after {round(now - start, 1)}s'
                )

            time.sleep(0.1)

    def status(self, cmd):
        """Report status of the most recent dotRoach visit."""
        df = self.engine.opdb.query_dataframe(
            'SELECT * FROM dot_roach_flux WHERE pfs_visit_id = '
            '(SELECT MAX(pfs_visit_id) FROM dot_roach_flux)'
        )
        if df.empty:
            cmd.inform('text="no dotRoach data available"')
            return

        visit = int(df.pfs_visit_id.iloc[0])
        cmd.inform(f'text="visit={visit}, nCobras={len(df)}"')
