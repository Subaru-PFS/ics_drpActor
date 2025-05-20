import logging
import time

import numpy as np
import pandas as pd
from lsst.ip.isr import AssembleCcdTask

config = AssembleCcdTask.ConfigClass()
config.doTrim = True
assembleTask = AssembleCcdTask(config=config)

from pfs.drp.stella.extractSpectraTask import ExtractSpectraTask

config = ExtractSpectraTask.ConfigClass()
config.doCrosstalk = False
config.validate()
extractSpectra = ExtractSpectraTask(config=config)


def getWindowedFluxes(exp, dataId, fiberTrace, detectorMap, darkVariance=30, **kwargs):
    """Return an estimate of the median flux in each fibre

     Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance for retrieving datasets.
    dataId : `dict`
        Dictionary specifying the exposure to process (e.g., exposure ID).
    fiberTrace : `FiberTrace`
        Fiber trace object containing fiber definitions and extraction profiles.
    detectorMap : `DetectorMap`
        Detector map object providing information about pixel-to-fiber mapping.
    darkVariance : `float`, optional
        Minimum variance value to be applied to prevent negative variance, by default 30.
    **kwargs : `dict`
        Additional overrides to update `dataId`.
    """
    start = time.time()
    dataId = dataId.copy()
    dataId.update(kwargs)

    try:
        md = exp.getMetadata()
        row0, row1 = md["W_CDROW0"], md["W_CDROWN"]

        maskVal = exp.mask.getPlaneBitMask(["SAT", "NO_DATA"])
        exp.mask.array[0:row0] = maskVal
        exp.mask.array[row1 + 1:] = maskVal
        exp.image.array[exp.mask.array != 0] = np.nan  # not 0; we're going to use np.nanmedian later

        for amp in exp.getDetector():
            exp[amp.getRawBBox()].image.array -= np.nanmedian(
                exp[amp.getRawHorizontalOverscanBBox()].image.array
            )

        exp = assembleTask.assembleCcd(exp)
        exp.variance = exp.image
        exp.variance.array += darkVariance

        spectra = extractSpectra.run(exp.maskedImage, fiberTrace, detectorMap).spectra.toPfsArm(dataId)
        spectra.flux[spectra.mask != 0] = np.nan

        df = pd.DataFrame(dict(flux=np.nanmedian(spectra.flux, axis=1), fiberId=spectra.fiberId))
        logging.info(f'{dataId} flux extracted in {round(time.time() - start, 1)}s')

    except Exception as e:
        logging.warning(f'Failed to extract flux for {dataId}: {type(e).__name__}: {e}')
        df = pd.DataFrame(columns=["flux", "fiberId"])

    return df
