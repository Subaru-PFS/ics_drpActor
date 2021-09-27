import numpy as np
import os
import pandas as pd
from pfs.utils.fiberids import FiberIds
from scipy.ndimage import median_filter

gfm = FiberIds()
gfmDf = pd.DataFrame(gfm.data)

scienceFiberId = np.arange(2394) + 1
scienceFiber = gfmDf.set_index('scienceFiberId').loc[scienceFiberId].reset_index()

spec1 = scienceFiber.query('spectrographId==1').sort_values('fiberId')
fiberIds = spec1.fiberId.to_numpy()
cobraIds = scienceFiber.set_index('fiberId').loc[fiberIds].cobraId.to_numpy()


def robustRms(array):
    """Calculate a robust RMS of the array using the inter-quartile range

    Uses the standard conversion of IQR to RMS for a Gaussian.

    Parameters
    ----------
    array : `numpy.ndarray`
        Array for which to calculate RMS.

    Returns
    -------
    rms : `float`
        Robust RMS.
    """
    lq, uq = np.percentile(array, (25.0, 75.0))
    return 0.741 * (uq - lq)


def robustSigmaClip(array, sigma=3):
    """Calculate a robust RMS of the array using the inter-quartile range"""
    centered = array - np.median(array)
    rms = robustRms(centered)
    mask = np.logical_and(centered > -sigma * rms, centered < sigma * rms)
    return ~mask


def boxCarExtraction(im, fiberTraces):
    """Calculate a robust RMS of the array using the inter-quartile range"""
    yc = np.arange(fiberTraces.shape[1], dtype='int64')
    ycc = np.tile(yc, (fiberTraces.shape[2], 1)).transpose()

    fluxes = []

    for i in range(fiberTraces.shape[0]):
        fluxes.append(im[ycc, fiberTraces[i]].sum(axis=1))

    return np.array(fluxes)


def robustFluxEstimation(im, nRows=1000, medWindowSize=41):
    """Calculate a robust RMS of the array using the inter-quartile range"""
    fiberTraces = np.load('/data/drp/fpsDotLoop/fiberTraces.npy')
    centerRow = fiberTraces.shape[1] / 2
    flux = boxCarExtraction(im, fiberTraces)
    box = flux[:, int(round(centerRow - nRows / 2)):int(round(centerRow + nRows / 2))]
    data = []

    for i, fiber in enumerate(fiberIds):
        noise = box[i] - median_filter(box[i], medWindowSize)
        mask = robustSigmaClip(noise, sigma=3)

        noiseLevel = np.std(noise[~mask])
        meanSignal = np.mean(box[i][~mask])
        centerFlux = np.sum(box[i][~mask])

        data.append((centerFlux, noiseLevel, meanSignal))

    df = pd.DataFrame(data, columns=['centerFlux', 'noiseLevel', 'meanSignal'])
    df['totalFlux'] = flux[:, 100:-100].sum(axis=1)
    df['fiberId'] = fiberIds
    df['cobraId'] = cobraIds
    df['fluxGradient'] = np.nan * len(df)
    df['keepMoving'] = np.ones(len(df), dtype=bool)

    return df.sort_values('cobraId')

