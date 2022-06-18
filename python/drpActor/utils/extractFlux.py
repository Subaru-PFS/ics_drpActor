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
config.validate()
extractSpectra = ExtractSpectraTask(config=config)


def getWindowedFluxes(butler, dataId, fiberTrace, darkVariance=30, **kwargs):
    """Return an estimate of the median flux in each fibre

    butler: a butler pointing at the data
    dataId: a dict specifying the desired data
    fiberTraces: a dict indexed by arm containing the appropriate FiberTrace objects
                 If None, read and construct the FiberTrace
    kwargs: overrides for dataId
    """
    start = time.time()
    dataId = dataId.copy()
    dataId.update(kwargs)
    detectorMap = butler.get("detectorMap", dataId)

    # if fiberTraces is None:
    #     fiberProfiles = butler.get("fiberProfiles", dataId)
    #     detectorMap = butler.get("detectorMap", dataId)
    #
    #     fiberTrace = fiberProfiles.makeFiberTracesFromDetectorMap(detectorMap)
    # else:
    #     fiberTrace = fiberTraces[dataId["arm"]]

    # pfsConfig = butler.get("pfsConfig", dataId)
    # pfsConfig = pfsConfig[pfsConfig.targetType != TargetType.ENGINEERING]

    exp = butler.get("raw", dataId, filter=dataId['arm'])
    exp = exp.convertF()

    md = exp.getMetadata()
    row0, row1 = md["W_CDROW0"], md["W_CDROWN"]

    maskVal = exp.mask.getPlaneBitMask(["SAT", "NO_DATA"])
    exp.mask.array[0:row0] = maskVal
    exp.mask.array[row1 + 1:] = maskVal
    exp.image.array[exp.mask.array != 0] = np.NaN  # not 0; we're going to use np.nanmedian later

    for amp in exp.getDetector():
        exp[amp.getRawBBox()].image.array -= np.nanmedian(exp[amp.getRawHorizontalOverscanBBox()].image.array)

    exp = assembleTask.assembleCcd(exp)
    exp.variance = exp.image
    exp.variance.array += darkVariance  # need a floor to the noise to reduce the b arm

    spectra = extractSpectra.run(exp.maskedImage, fiberTrace, detectorMap).spectra.toPfsArm(dataId)
    spectra.flux[spectra.mask != 0] = np.NaN

    df = pd.DataFrame(dict(flux=np.nanmedian(spectra.flux, axis=1), fiberId=spectra.fiberId))
    # calculating time.
    totalTime = round(time.time() - start, 1)
    logging.info(f'{dataId} flux extracted in {totalTime}s')
    return df
