import logging
import multiprocessing
import os
import tempfile
import time

import pandas as pd
from drpActor.utils.extractFlux import applyQuickISR, extractSpectraFromExp, getSpectraRatio
from pfs.datamodel import PfsArm, FiberStatus
from pfs.drp.stella.DetectorMapContinued import DetectorMap
from pfs.drp.stella.FiberTraceSetContinued import FiberTraceSet
from pfs.drp.stella.adjustDetectorMap import AdjustDetectorMapTask
from pfs.drp.stella.centroidTraces import CentroidTracesTask, tracesToLines


def extractRatioWorker(exp, dataId, fiberTrace, detectorMap, refSpectra, results):
    """Extract flux ratio for one camera and append to shared results list (runs in subprocess)."""
    df, _ = getSpectraRatio(exp, dataId, fiberTrace, detectorMap, refFlux=refSpectra.flux)
    results.append(df)


def buildCameraCalibration(dataId, exposure, fiberProfiles, detectorMap, pfsConfig, tmpDir):
    """Adjust detectorMap, build fiberTrace, and extract reference spectra for one camera (runs in subprocess).

    All inputs are pre-loaded by the caller. Results are written to tmpDir to avoid
    pickling LSST objects across processes:
      - detectorMap_{arm}{spec}.fits
      - pfsFiberTrace-2000-01-01-000000-{arm}{spec}.fits
      - pfsArm_{arm}{spec}.fits
    """
    arm, spec = dataId['arm'], dataId['spectrograph']
    t0 = time.time()
    logging.info(f'[{arm}{spec}] building camera calibration...')

    postIsrExp = applyQuickISR(exposure.convertF())
    traces = CentroidTracesTask().run(postIsrExp, detectorMap, pfsConfig=pfsConfig)
    lines = tracesToLines(detectorMap, traces, spectralError=5.0)
    detectorMap = AdjustDetectorMapTask().run(detectorMap, lines, arm, postIsrExp.visitInfo).detectorMap
    logging.info(f'[{arm}{spec}] detectorMap adjusted ({len(lines)} lines)')
    fiberTrace = fiberProfiles.makeFiberTracesFromDetectorMap(detectorMap)
    logging.info(f'[{arm}{spec}] fiberTrace built ({len(fiberTrace)} traces)')

    detectorMap.writeFits(os.path.join(tmpDir, f'detectorMap_{arm}{spec}.fits'))
    fiberTrace.writeFits(os.path.join(tmpDir, f'pfsFiberTrace-2000-01-01-000000-{arm}{spec}.fits'))

    spectra = extractSpectraFromExp(exposure.convertF(), dataId, fiberTrace, detectorMap)
    spectra.writeFits(os.path.join(tmpDir, f'pfsArm_{arm}{spec}.fits'))
    logging.info(f'[{arm}{spec}] camera calibration done in {round(time.time() - t0, 1)}s')


class DotRoach(object):
    processTimeout = 25 + 120

    def __init__(self, engine, cams):
        """Placeholder to handle DotRoach loop."""
        self.engine = engine
        self.processManager = multiprocessing.Manager()
        self.cams = cams

        self.currentVisit = None
        self.pfsConfig = None     # kept for the BROKENCOBRA lamp-monitor fibres
        self.detectorMaps = dict()
        self.fiberTraces = dict()
        self.refSpectra = dict()  # cameraKey -> PfsArm

    @property
    def monitoringFiberIds(self):
        # BROKENCOBRA fibres are parked at home and fully illuminated — use them as lamp monitors.
        return list(self.pfsConfig[self.pfsConfig.fiberStatus == FiberStatus.BROKENCOBRA].fiberId)

    def lampFactor(self, fluxPerFiber):
        """Per-visit lamp factor = median flux ratio of the parked BROKENCOBRA monitor fibres.

        Since those fibres are always fully lit, their flux_ratio (flux / reference-spectra)
        equals the lamp brightness relative to the reference flat.  Returns 1.0 if no monitors.
        """
        mon = fluxPerFiber.set_index('fiberId').reindex(self.monitoringFiberIds).dropna(subset=['flux_norm'])
        return float(mon.flux_norm.median()) if len(mon) else 1.0

    def getCamFiles(self, pfsVisit):
        return [f for f in pfsVisit.exposureFiles if f.cam in self.cams]

    def isVisitComplete(self, pfsVisit):
        """Return True when all expected cam files are present."""
        return len(self.getCamFiles(pfsVisit)) == len(self.cams)

    def run(self, pfsVisit):
        """Run method using pfsVisit."""
        camFiles = self.getCamFiles(pfsVisit)
        if not camFiles:
            return

        self.runAway(pfsVisit.visit, camFiles)

    def collectFiberData(self, files):
        """Retrieve raw exposures from butler and return flux ratio per fiber."""
        results = self.processManager.list()
        jobs = []

        exps = {file.cam: self.engine.butler.get('raw.exposure', file.dataId) for file in files}

        if not self.fiberTraces:
            logging.info(f'dotRoach: initializing calibrations for {[f.cam for f in files]}')
            self.initializeCalibrations(files, exps)
            logging.info('dotRoach: calibrations ready')

        for file in files:
            fiberTrace, detectorMap, refSpectra = self.getCameraCalibrations(file.dataId)
            exp = exps[file.cam]
            p = multiprocessing.Process(target=extractRatioWorker,
                                        args=(exp.convertF(), file.dataId, fiberTrace, detectorMap,
                                              refSpectra, results))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        return pd.concat(results).groupby('fiberId').first().reset_index()

    def initializeCalibrations(self, files, exps):
        """Build and cache fiberTraces and reference flux for all cameras in parallel.

        fiberTraces are built from the full-frame flat (passed as the first exposure
        in the dot-roach sequence).  The same exposure is used to extract refFlux —
        the per-fiber, per-wavelength reference against which subsequent windowed
        exposures are ratioed.

        Butler reads happen in the main process. Per-camera CPU work is parallelized
        via subprocesses; fiberTrace/detectorMap results are exchanged via FITS files
        to avoid pickling LSST objects.
        """
        pfsConfig = self.engine.butler.get('pfsConfig', files[0].dataId)
        self.pfsConfig = pfsConfig     # for the BROKENCOBRA lamp-monitor fibres

        with tempfile.TemporaryDirectory() as tmpDir:
            jobs = []
            for file in files:
                fiberProfiles = self.engine.butler.get('fiberProfiles', file.dataId)
                detectorMap = self.engine.butler.get('detectorMap_calib', file.dataId)
                p = multiprocessing.Process(target=buildCameraCalibration,
                                            args=(file.dataId, exps[file.cam], fiberProfiles,
                                                  detectorMap, pfsConfig, tmpDir))
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
                self.refSpectra[cameraKey] = PfsArm.readFits(os.path.join(tmpDir, f'pfsArm_{arm}{spec}.fits'))

    def getCameraCalibrations(self, dataId):
        """Return the cached (fiberTrace, detectorMap, refSpectra) for the given camera."""
        cameraKey = dataId['spectrograph'], dataId['arm']
        return self.fiberTraces[cameraKey], self.detectorMaps[cameraKey], self.refSpectra[cameraKey]

    def runAway(self, visit, files):
        """Extract flux ratio, lamp-normalize, and bulk-insert into opdb."""
        self.currentVisit = visit
        t0 = time.time()
        logging.info(f'dotRoach: processing visit={visit} cams={[f.cam for f in files]}')

        fluxPerFiber = self.collectFiberData(files)

        # Lamp factor from the parked BROKENCOBRA monitor fibres (always fully lit).
        lampFactor = self.lampFactor(fluxPerFiber)

        # Drop engineering fibers — only cobras go to the DB.  Columns:
        #   flux_abs        raw extracted flux
        #   flux_ratio      flux / reference-spectra (per-cobra hide ratio)
        #   flux_ratio_norm flux_ratio / lamp factor (monitor-corrected)
        fluxPerCobra = fluxPerFiber[fluxPerFiber.cobraId != -1].copy()
        fluxPerCobra['pfs_visit_id'] = visit
        fluxPerCobra['flux_ratio_norm'] = fluxPerCobra.flux_norm.to_numpy() / lampFactor
        fluxPerCobra = fluxPerCobra.rename(columns={'cobraId': 'cobra_id', 'flux': 'flux_abs',
                                                    'flux_norm': 'flux_ratio'})[
            ['pfs_visit_id', 'cobra_id', 'flux_abs', 'flux_ratio', 'flux_ratio_norm']]

        self.engine.opdb.insert('dot_roach_flux', fluxPerCobra)
        logging.info(f'dotRoach: visit={visit} inserted {len(fluxPerCobra)} rows '
                     f'(lamp={round(lampFactor, 3)}) in {round(time.time() - t0, 1)}s')

    def waitForResult(self, cmd):
        """Wait until dot_roach_flux rows for the current visit appear in opdb."""
        start = time.time()
        lastReport = start

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

            if now - lastReport >= 5:
                cmd.inform(f'text="dotRoach: waiting for visit={self.currentVisit} ({round(now - start)}s elapsed)"')
                lastReport = now

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
