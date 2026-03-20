import logging
import multiprocessing
import os
import tempfile
import time

import pandas as pd
from drpActor.utils.extractFlux import applyQuickISR, getWindowedFluxes
from pfs.datamodel.pfsConfig import FiberStatus
from pfs.drp.stella.adjustDetectorMap import AdjustDetectorMapTask
from pfs.drp.stella.centroidTraces import CentroidTracesTask, tracesToLines
from pfs.drp.stella.DetectorMapContinued import DetectorMap
from pfs.drp.stella.FiberTraceSetContinued import FiberTraceSet
from pfs.utils.fiberids import FiberIds


def extractFluxWorker(exp, dataId, fiberTrace, detectorMap, fluxPerFiber):
    """Extract windowed flux for one camera and append to shared results list (runs in subprocess)."""
    fluxPerFiber.append(getWindowedFluxes(exp, dataId, fiberTrace=fiberTrace, detectorMap=detectorMap))


def buildFiberTrace(dataId, exposure, fiberProfiles, detectorMap, pfsConfig, tmpDir):
    """Adjust detectorMap and build fiberTrace for one camera (runs in subprocess).

    All inputs are pre-loaded by the caller. Results are written to FITS files in
    tmpDir (keyed by arm+spectrograph) to avoid pickling LSST objects across processes.
    """
    arm, spec = dataId['arm'], dataId['spectrograph']
    logging.info(f'building fiberTrace for {arm}{spec}')

    postIsrExp = applyQuickISR(exposure.convertF())
    traces = CentroidTracesTask().run(postIsrExp, detectorMap, pfsConfig=pfsConfig)
    lines = tracesToLines(detectorMap, traces, spectralError=5.0)
    detectorMap = AdjustDetectorMapTask().run(detectorMap, lines, arm, postIsrExp.visitInfo).detectorMap
    fiberTrace = fiberProfiles.makeFiberTracesFromDetectorMap(detectorMap)

    detectorMap.writeFits(os.path.join(tmpDir, f'detectorMap_{arm}{spec}.fits'))
    fiberTrace.writeFits(os.path.join(tmpDir, f'pfsFiberTrace-2000-01-01-000000-{arm}{spec}.fits'))


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

        # Load exposures upfront — needed for detectorMap adjustment and parallel flux extraction.
        exps = {file.cam: self.engine.butler.get('raw.exposure', file.dataId) for file in files}

        if not self.fiberTraces:
            self.initializeCalibrations(files, exps)

        for file in files:
            fiberTrace, detectorMap = self.getFiberTrace(file.dataId)
            exp = exps[file.cam]
            p = multiprocessing.Process(target=extractFluxWorker,
                                        args=(exp.convertF(), file.dataId, fiberTrace, detectorMap, fluxPerFiber))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        return pd.concat(fluxPerFiber).groupby('fiberId').sum().reset_index()

    def initializeCalibrations(self, files, exps):
        """Build and cache fiberTraces for all cameras in parallel.

        Butler reads happen in the main process. Per-camera CPU work (ISR, trace
        centroiding, detectorMap adjustment) is parallelized. Results are exchanged
        via FITS files to avoid pickling LSST objects across processes.
        """
        self.pfsConfig = self.engine.butler.get('pfsConfig', files[0].dataId)

        with tempfile.TemporaryDirectory() as tmpDir:
            jobs = []
            for file in files:
                fiberProfiles = self.engine.butler.get('fiberProfiles', file.dataId)
                detectorMap = self.engine.butler.get('detectorMap_calib', file.dataId)
                p = multiprocessing.Process(target=buildFiberTrace,
                                            args=(file.dataId, exps[file.cam], fiberProfiles,
                                                  detectorMap, self.pfsConfig, tmpDir))
                jobs.append(p)
                p.start()
            for p in jobs:
                p.join()

            for file in files:
                arm, spec = file.dataId['arm'], file.dataId['spectrograph']
                cameraKey = (spec, arm)
                self.detectorMaps[cameraKey] = DetectorMap.readFits(
                    os.path.join(tmpDir, f'detectorMap_{arm}{spec}.fits'))
                self.fiberTraces[cameraKey] = FiberTraceSet.readFits(
                    os.path.join(tmpDir, f'pfsFiberTrace-2000-01-01-000000-{arm}{spec}.fits'))

    def getFiberTrace(self, dataId):
        """Return the cached (fiberTrace, detectorMap) for the given camera."""
        cameraKey = dataId['spectrograph'], dataId['arm']
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
