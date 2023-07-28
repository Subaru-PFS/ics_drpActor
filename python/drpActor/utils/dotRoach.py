import logging
import multiprocessing
import os
import time

import drpActor.utils.extractFlux as extractFlux
import numpy as np
import pandas as pd
from pfs.utils.fiberids import FiberIds


class DotRoach(object):
    gfm = pd.DataFrame(FiberIds().data)
    sgfm = gfm[gfm.cobraId != FiberIds.MISSING_VALUE]
    processTimeout = 15

    def __init__(self, engine, dataRoot, maskFile, keepMoving=False):
        """ Placeholder to handle DotRoach loop"""
        self.engine = engine
        self.processManager = multiprocessing.Manager()

        self.pathDict = self.initialise(dataRoot)
        self.maskFile = pd.read_csv(maskFile, index_col=0).sort_values('cobraId')
        self.didOvershoot = self.allStoppedMaskFile()
        self.keepMoving = keepMoving

        self.normFactor = None
        self.maxIterInPhase1 = -1
        self.phase = 'phase1'

        self.detectorMaps = dict()
        self.fiberTraces = dict()

    @property
    def monitoringFiberIds(self):
        # bitMask 0, means at Home. disabled / broken should already be at home.
        atHome = self.maskFile[self.maskFile.bitMask == 0].fiberId
        return list(atHome)

    @property
    def strategy(self):
        return self.phase[:6]

    def loadAllIterations(self):
        """Load allIterations dataframe."""
        return pd.read_csv(self.pathDict['allIterations'], index_col=0)

    def initialise(self, dataRoot):
        """Create output directory and files"""
        # construct path
        maskFilesRoot = os.path.join(dataRoot, 'maskFiles')
        outputPath = os.path.join(dataRoot, 'allIterations.csv')

        # creating repo
        for newDir in [dataRoot, maskFilesRoot]:
            os.mkdir(newDir)

        # initialising output file.
        pd.DataFrame([]).to_csv(outputPath)

        return dict(dataRoot=dataRoot, allIterations=outputPath, maskFilesRoot=maskFilesRoot)

    def allStoppedMaskFile(self):
        """Build final move mask."""
        maskFile = self.maskFile.copy()
        maskFile['bitMask'] = np.zeros(len(maskFile)).astype('int')
        return maskFile

    def collectFiberData(self, files):
        """Retrieve pfsArm from butler and return flux estimation for each fiber."""
        fluxPerFiber = self.processManager.list()
        jobs = []

        def parallelize(butler, dataId, fiberTrace, detectorMap, fluxPerFiber):
            flux = extractFlux.getWindowedFluxes(butler, dataId,
                                                 fiberTrace=fiberTrace, detectorMap=detectorMap, useButler=True)
            fluxPerFiber.append(flux)

        # load fiberTraces on first iteration presumably.
        if not self.fiberTraces:
            for file in files:
                self.getFiberTrace(file.dataId)

        for file in files:
            fiberTrace, detectorMap = self.getFiberTrace(file.dataId)
            p = multiprocessing.Process(target=parallelize,
                                        args=(self.engine.butler, file.dataId, fiberTrace, detectorMap, fluxPerFiber))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        return pd.concat(fluxPerFiber).groupby('fiberId').sum().reset_index()

    def getFiberTrace(self, dataId):
        """Retrieve fiberTrace"""
        cameraKey = dataId['spectrograph'], dataId['arm']

        if cameraKey not in self.fiberTraces:
            logging.info(f'making fiberTrace for {cameraKey}')
            fiberProfiles = self.engine.butler.get("fiberProfiles", dataId)
            detectorMap = self.engine.butler.get("detectorMap", dataId)
            self.detectorMaps[cameraKey] = detectorMap
            self.fiberTraces[cameraKey] = fiberProfiles.makeFiberTracesFromDetectorMap(detectorMap)

        return self.fiberTraces[cameraKey], self.detectorMaps[cameraKey]

    def runAway(self, files):
        """Append new iteration to dataset."""

        def fiberToCobra(fluxPerFiber):
            """Convert flux per fiber as flux per cobra."""
            fluxPerCobra = DotRoach.gfm.set_index('fiberId').loc[fluxPerFiber.fiberId][['cobraId']].copy()
            fluxPerCobra['flux'] = fluxPerFiber.flux.to_numpy()
            fluxPerCobra = fluxPerCobra[fluxPerCobra.cobraId != FiberIds.MISSING_VALUE]
            return fluxPerCobra.reset_index().sort_values('cobraId')

        def buildIterData(fluxPerCobra):
            """Build an iteration file with an entry for each cobra."""
            # copy config as based structured file.
            newIter = self.maskFile.copy()
            # initialise flux to NaN
            flux = np.ones(len(newIter)) * np.NaN
            # fill with measured flux
            flux[fluxPerCobra.cobraId.to_numpy() - 1] = fluxPerCobra.flux.to_numpy()
            newIter['flux'] = flux

            return newIter

        def toMaskFile(lastIter):
            """Convert last iteration to cobra maskFile."""
            maskFile = self.maskFile.copy()
            maskFile['bitMask'] = lastIter.keepMoving.to_numpy().astype('int')
            return maskFile

        # build dataset with flux measurement for all cobras.
        fluxPerFiber = self.collectFiberData(files)
        fluxPerCobra = fiberToCobra(fluxPerFiber)
        newIter = buildIterData(fluxPerCobra)

        # adding visit
        [visit] = list(set([file.visit for file in files]))
        newIter['visit'] = visit

        # compute normalized flux using monitoring fibers.
        newIter['fluxNorm'] = self.fluxNormalized(newIter)

        # update output file with the new iteration data
        allIterations = self.process(newIter)
        allIterations.to_csv(self.pathDict['allIterations'])

        # export maskFiles for fps
        nIter = allIterations.nIter.max()
        lastIter = allIterations.query(f'nIter=={nIter}').sort_values('cobraId')
        maskFile = toMaskFile(lastIter)
        maskFile.to_csv(os.path.join(self.pathDict['maskFilesRoot'], f'iter{nIter}.csv'))

    def fluxNormalized(self, newIter):
        """Return flux normalized by the lamp response."""

        def fluxFromMonitoringFibers():
            """Calculate flux from homed/disabled cobras."""
            monitoring = newIter.set_index('fiberId').reindex(self.monitoringFiberIds).dropna().flux.sum()
            # setting flux to 1 in case I have actually no monitoring fibers available.
            monitoring = 1 if not monitoring else monitoring
            return monitoring

        lampResponse = fluxFromMonitoringFibers()

        if self.normFactor is None:
            self.normFactor = lampResponse

        normFactor = self.normFactor / lampResponse
        return newIter.flux.to_numpy() * normFactor

    def process(self, newIter):
        """Process new iteration, namely decide which cobras need to stop moving."""

        def shouldIStop(fluxRatio, goalInPhase1=0.003):
            """"""
            gradient = fluxRatio[-1] - fluxRatio[-2]
            gain = gradient / fluxRatio[-1]
            ratioInPhase1 = fluxRatio[:(self.maxIterInPhase1 + 1)]  # per python excluding upper boundary.

            if self.strategy == 'phase1' and fluxRatio[-1] < goalInPhase1:
                flag = 1
            elif self.strategy == 'phase2' and (gain < 0.05 and fluxRatio[-1] < np.min(ratioInPhase1)):
                flag = 1
            elif fluxRatio.min() < 0.5 and gradient > 0:
                flag = 2
            else:
                flag = 0

            return flag

        allIterations = self.loadAllIterations()

        # first iteration
        if allIterations.empty:
            newIter['keepMoving'] = self.maskFile.bitMask.astype('bool')
            newIter['nIter'] = int(0)
            return newIter

        # We do not have any criteria left, so force keepMoving for now.
        keepMoving = np.ones(len(newIter), dtype='bool')

        # logical and with previous iteration
        lastIter = allIterations.query(f'visit=={allIterations.visit.max()}').sort_values('cobraId')

        prevState = lastIter.keepMoving
        keepMoving = np.logical_and(prevState, keepMoving).to_numpy()
        nIter = lastIter.nIter.to_numpy() + 1

        for cobraId, df in allIterations.groupby('cobraId'):
            df = df.sort_values('nIter')
            new = newIter.set_index('cobraId').loc[cobraId]
            iCob = cobraId - 1
            cobraStopped = not keepMoving[iCob]
            if cobraStopped:
                continue

            fluxCobra = np.append(df.fluxNorm.to_numpy(), new.fluxNorm)
            fluxRatio = fluxCobra / fluxCobra[0]
            flag = shouldIStop(fluxRatio)

            self.didOvershoot.bitMask[iCob] = int(flag == 2)
            keepMoving[iCob] = flag == 0

        if self.phase == 'phase1->phase2':
            self.maxIterInPhase1 = max(nIter)
            self.phase = 'phase2'
            keepMoving = self.didOvershoot.bitMask.astype('bool').to_numpy()

        elif self.phase == 'phase2->phase3':
            self.phase = 'phase3'
            keepMoving = self.didOvershoot.bitMask.astype('bool').to_numpy()

        newIter['keepMoving'] = keepMoving
        newIter['nIter'] = nIter

        # append to current allIterations
        allIterations = pd.concat([allIterations, newIter]).reset_index(drop=True)
        return allIterations

    def finish(self):
        """ """
        rootDir, __ = os.path.split(self.pathDict["dataRoot"])
        # renaming current to dedicated path.
        visitMin = self.loadAllIterations().visit.min()
        os.rename(self.pathDict["dataRoot"], os.path.join(rootDir, f'v{str(visitMin).zfill(6)}'))

    def phase2(self):
        """"""
        self.phase = 'phase1->phase2'

    def phase3(self):
        """"""
        self.phase = 'phase2->phase3'

    def waitForResult(self, iteration):
        """Wait for maskFile to be created."""
        overHead = 20 if iteration == 0 else 0
        iterFile = os.path.join(self.pathDict['maskFilesRoot'], f'iter{iteration}.csv')
        start = time.time()

        while not os.path.isfile(iterFile):
            now = time.time()

            if (now - start) > (DotRoach.processTimeout + overHead):
                raise RuntimeError(f'no results after {round(now - start, 1)}')

            time.sleep(0.1)

    def status(self, cmd):
        """ """
        cmd.inform(f"dotRoach={self.pathDict['allIterations']}")

        allIterations = self.loadAllIterations()
        lastVisit = allIterations.visit.max()
        lastIter = allIterations.query(f'visit=={lastVisit}').sort_values('cobraId')

        cmd.inform(f'text="visit={lastVisit}, nCobraKeepMoving={len(lastIter[lastIter.keepMoving])}"')
