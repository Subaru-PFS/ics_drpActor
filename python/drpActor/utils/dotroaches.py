import os
import pathlib

import numpy as np
import pandas as pd
from ics.cobraCharmer import pfiDesign
from pfs.utils.fiberids import FiberIds

gfm = FiberIds()
gfmDf = pd.DataFrame(gfm.data)

scienceFiberId = np.arange(2394) + 1
scienceFiber = gfmDf.set_index('scienceFiberId').loc[scienceFiberId].reset_index()

pfi = pfiDesign.PFIDesign(fileName=
pathlib.Path(
    '/software/devel/cpl/pfs_instdata/data/pfi/modules/ALL/ALL_final_20210920_mm.xml'))

disabledOrBrokenCobras = pfi.status != pfi.COBRA_OK_MASK
disabled = scienceFiber[disabledOrBrokenCobras].sort_values('cobraId')


class DotRoaches(object):
    """ Placeholder to handle dotRoaches loop"""

    def __init__(self, engine):
        self.filepath = None
        self.engine = engine

    def collectFiberData(self, files):
        """ retrieve pfsArm from butler and return flux estimation for each fiber.
        :param files:
        :return:
        """
        fluxPerFiber = []

        for file in files:
            pfsArm = self.engine.butler.get('pfsArm', **file.dataId)
            fluxPerFiber.append(pd.DataFrame(dict(flux=pfsArm.flux.sum(axis=1), fiberId=pfsArm.fiberId)))

        return pd.concat(fluxPerFiber).groupby('fiberId').sum().reset_index()

    def runAway(self, files):
        """
        :param files:
        :return:
        """

        def fiberToCobra(fluxPerFiber):
            """ convert flux per fiber as flux per cobra.
            :param fluxPerFiber: 
            :return: 
            """""
            fluxPerCobra = gfmDf.set_index('fiberId').loc[fluxPerFiber.fiberId][['cobraId']]
            fluxPerCobra['flux'] = fluxPerFiber.flux.to_numpy()
            fluxPerCobra = fluxPerCobra[fluxPerCobra.cobraId != FiberIds.MISSING_VALUE]
            return fluxPerCobra.sort_values('cobraId').reset_index()

        fluxPerFiber = self.collectFiberData(files)
        fluxPerCobra = fiberToCobra(fluxPerFiber)

        [visit] = list(set([file.visit for file in files]))
        fluxPerCobra['visit'] = visit

        if self.filepath is None:
            self.filepath = os.path.join(self.engine.outputDir, 'dotroaches', f'{visit}.csv')
            cobraMotion = self.initialise(fluxPerCobra)
        else:
            cobraMotion = self.newCobraIteration(fluxPerCobra)

        cobraMotion.to_csv(self.filepath)

    def initialise(self, fluxPerCobra):
        """ initalise cobra file.
        :param fluxPerCobra:
        :return:
        """
        self.normFactor = self.fluxFromDisabledCobra(fluxPerCobra)

        fluxPerCobra['fluxNorm'] = fluxPerCobra.flux
        fluxPerCobra['fluxGradient'] = [np.nan] * len(fluxPerCobra)
        fluxPerCobra['keepMoving'] = np.ones(len(fluxPerCobra), dtype=bool)

        return fluxPerCobra

    def newCobraIteration(self, fluxPerCobra):
        """  calculate flux gradient and return full cobra motion file.
        :param fluxPerCobra:
        :return:
        """
        # Calculate norm flux from disabled cobras
        normFactor = self.normFactor / self.fluxFromDisabledCobra(fluxPerCobra)
        fluxPerCobra['fluxNorm'] = fluxPerCobra.flux * normFactor

        # reload file
        allMotions = pd.read_csv(self.filepath, index_col=0)
        last = allMotions.set_index('visit').loc[allMotions.visit.max()].sort_values('cobraId').reset_index()

        # calculate flux gradient
        newFlux = fluxPerCobra.fluxNorm.to_numpy()
        lastFlux = last.fluxNorm.to_numpy()
        fluxPerCobra['fluxGradient'] = newFlux - lastFlux

        # assess which cobra should keep moving
        keepMoving = fluxPerCobra.fluxGradient < 0
        keepMoving = np.logical_and(last.keepMoving, keepMoving).to_numpy()
        fluxPerCobra['keepMoving'] = keepMoving

        # append to file
        allMotions = pd.concat([allMotions, fluxPerCobra]).reset_index(drop=True)
        return allMotions

    def fluxFromDisabledCobra(self, fluxPerCobra):
        """ Calculate flux from disabled cobra
        :param fluxPerCobra:
        :return:
        """
        return fluxPerCobra.set_index('cobraId').reindex(disabled.cobraId).dropna().sum().flux

    def finish(self):
        """ """
        pass
