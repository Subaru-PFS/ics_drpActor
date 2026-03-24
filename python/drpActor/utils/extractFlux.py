import logging
import time

import numpy as np
import pandas as pd
from lsst.ip.isr import AssembleCcdTask
from pfs.datamodel import Identity
from pfs.utils.fiberids import FiberIds

config = AssembleCcdTask.ConfigClass()
config.doTrim = True
assembleTask = AssembleCcdTask(config=config)

from pfs.drp.stella.extractSpectraTask import ExtractSpectraTask

config = ExtractSpectraTask.ConfigClass()
config.doCrosstalk = False
config.validate()
extractSpectra = ExtractSpectraTask(config=config)

gfm = FiberIds()


def applyQuickISR(exp, darkVariance=30):
    """Overscan-subtract per amp and assemble CCD (no windowing).

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        Raw float exposure (call convertF() before passing in).
    darkVariance : `float`, optional
        Variance floor added after assembly to suppress read-noise artefacts.

    Returns
    -------
    exp : `lsst.afw.image.Exposure`
        Assembled, overscan-subtracted exposure ready for trace centroiding
        or spectrum extraction.
    """
    for amp in exp.getDetector():
        exp[amp.getRawBBox()].image.array -= np.nanmedian(exp[amp.getRawHorizontalOverscanBBox()].image.array)

    exp = assembleTask.assembleCcd(exp)
    exp.variance = exp.image
    exp.variance.array += darkVariance
    return exp


def extractSpectraFromExp(exp, dataId, fiberTrace, detectorMap, darkVariance=30, **kwargs):
    """Apply quick ISR, mask outside the readout window, and extract spectra.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        Raw float exposure (call convertF() before passing in).
    dataId : `dict`
        Butler data identifier for this exposure.
    fiberTrace : `FiberTraceSet`
        Fiber trace object for spectrum extraction.
    detectorMap : `DetectorMap`
        Detector map providing pixel-to-fiber mapping.
    darkVariance : `float`, optional
        Variance floor added after ISR.
    **kwargs
        Additional key=value overrides applied to dataId.

    Returns
    -------
    spectra : `PfsArm`
        Extracted spectra; flux outside the readout window is NaN.
    """
    start = time.time()
    dataId = dataId.copy()
    dataId.update(kwargs)

    md = exp.getMetadata()
    row0, row1 = md["W_CDROW0"], md["W_CDROWN"]

    maskVal = exp.mask.getPlaneBitMask(["SAT", "NO_DATA"])
    exp.mask.array[0:row0] = maskVal
    exp.mask.array[row1 + 1:] = maskVal
    exp.image.array[exp.mask.array != 0] = np.nan

    exp = applyQuickISR(exp, darkVariance=darkVariance)

    spectra = extractSpectra.run(exp.maskedImage, fiberTrace, detectorMap).spectra.toPfsArm(Identity(**dataId))
    spectra.flux[spectra.mask != 0] = np.nan

    logging.info(f'{dataId} spectra extracted in {round(time.time() - start, 1)}s')
    return spectra


def getSpectraRatio(exp, dataId, fiberTrace, detectorMap, darkVariance=30, refFlux=None, **kwargs):
    """Extract spectra and return per-fiber flux and flux ratio against a reference.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        Raw float exposure (call convertF() before passing in).
    dataId : `dict`
        Butler data identifier for this exposure.
    fiberTrace : `FiberTraceSet`
        Fiber trace object for spectrum extraction.
    detectorMap : `DetectorMap`
        Detector map providing pixel-to-fiber mapping.
    darkVariance : `float`, optional
        Variance floor added after ISR.
    refFlux : `np.ndarray` or None
        Reference flux array, shape (nFibers, nWavelength), indexed in the
        same fiber order as the extracted spectra.  When None the exposure
        is its own reference (flux_norm == 1 everywhere).
    **kwargs
        Additional key=value overrides applied to dataId.

    Returns
    -------
    df : `pd.DataFrame`
        Columns: fiberId, cobraId, flux, flux_norm.
        flux      — nanmedian of the extracted flux (window rows only).
        flux_norm — nanmedian of spectra.flux / refFlux (window rows only);
                    values < 1 indicate a cobra partially or fully hidden.
    flux : `np.ndarray`
        Raw flux array (nFibers, nWavelength) for use as refFlux on subsequent calls.
    """
    spectra = extractSpectraFromExp(exp, dataId, fiberTrace, detectorMap, darkVariance=darkVariance, **kwargs)

    ref = spectra.flux if refFlux is None else refFlux

    df = pd.DataFrame(dict(
        fiberId=spectra.fiberId,
        cobraId=gfm.fiberIdToCobraId(spectra.fiberId),
        flux=np.nanmedian(spectra.flux, axis=1),
        flux_norm=np.nanmedian(spectra.flux / ref, axis=1),
    ))

    return df, spectra.flux
