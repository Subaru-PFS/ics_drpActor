import logging
import os

import drpActor.utils.extractFlux as extractFlux
import numpy as np
import pandas as pd
from pfs.utils.fiberids import FiberIds


class DotRoach(object):
    gfm = pd.DataFrame(FiberIds().data)
    sgfm = gfm[gfm.cobraId != FiberIds.MISSING_VALUE]

    def __init__(self, engine, dataRoot, maskFile, keepMoving=False):
        """ Placeholder to handle DotRoach loop"""
        self.engine = engine

        self.pathDict = self.initialise(dataRoot)
        self.maskFile = pd.read_csv(maskFile, index_col=0).sort_values('cobraId')
        self.keepMoving = keepMoving
        self.normFactor = None

        self.detectorMaps = dict()
        self.fiberTraces = dict()

    @property
    def monitoringFiberIds(self):
        # bitMask 0, means at Home. disabled / broken should already be at home.
        atHome = self.maskFile[self.maskFile.bitMask == 0].fiberId
        return list(atHome)

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

    def collectFiberData(self, files):
        """Retrieve pfsArm from butler and return flux estimation for each fiber."""
        fluxPerFiber = []

        # load fiberTraces on first iteration presumably.
        if not self.fiberTraces:
            for file in files:
                self.getFiberTrace(file.dataId)

        # now we can safely deactivate autoIngest to save time.
        self.engine.doAutoIngest = False

        for file in files:
            fiberTrace, detectorMap = self.getFiberTrace(file.dataId)
            flux = extractFlux.getWindowedFluxes(self.engine.butler, file.dataId,
                                                 fiberTrace=fiberTrace, detectorMap=detectorMap, useButler=False)
            fluxPerFiber.append(flux)

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
            return newIter.set_index('fiberId').reindex(self.monitoringFiberIds).dropna().flux.sum()

        lampResponse = fluxFromMonitoringFibers()

        if self.normFactor is None:
            self.normFactor = lampResponse

        normFactor = self.normFactor / lampResponse
        return newIter.flux.to_numpy() * normFactor

    def process(self, newIter):
        """Process new iteration, namely decide which cobras need to stop moving."""

        def shouldIStop(fluxRatio, goal=0.03):
            if fluxRatio[-1] < goal:
                return True

            if fluxRatio.min() < 0.5:
                if fluxRatio[-1] - fluxRatio[-2] > 0:
                    return True

            return False

        allIterations = self.loadAllIterations()

        # first iteration
        if allIterations.empty:
            newIter['keepMoving'] = self.maskFile.bitMask.astype('bool')
            newIter['nIter'] = int(0)
            return newIter

        # We dont have any criteria left, so force keepMoving for now.
        keepMoving = np.ones(len(newIter), dtype='bool')

        # logical and with previous iteration
        lastIter = allIterations.query(f'visit=={allIterations.visit.max()}').sort_values('cobraId')
        keepMoving = np.logical_and(lastIter.keepMoving, keepMoving).to_numpy()

        for cobraId, df in allIterations.groupby('cobraId'):
            df = df.sort_values('nIter')
            new = lastIter.set_index('cobraId').loc[cobraId]
            iCob = cobraId - 1
            cobraStopped = not keepMoving[iCob]
            if cobraStopped:
                continue

            fluxCobra = np.append(df.fluxNorm.to_numpy(), new.fluxNorm)
            fluxRatio = fluxCobra / fluxCobra[0]
            keepMoving[iCob] = not shouldIStop(fluxRatio)

        newIter['keepMoving'] = keepMoving
        newIter['nIter'] = lastIter.nIter.to_numpy() + 1

        # append to current allIterations
        allIterations = pd.concat([allIterations, newIter]).reset_index(drop=True)
        return allIterations

    def finish(self):
        """ """
        pass

    def status(self, cmd):
        """ """
        cmd.inform(f"dotRoach={self.pathDict['allIterations']}")

        allIterations = self.loadAllIterations()
        lastVisit = allIterations.visit.max()
        lastIter = allIterations.query(f'visit=={lastVisit}').sort_values('cobraId')

        cmd.inform(f'text="visit={lastVisit}, nCobraKeepMoving={len(lastIter[lastIter.keepMoving])}"')
