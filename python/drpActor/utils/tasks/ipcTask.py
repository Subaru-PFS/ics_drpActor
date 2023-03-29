#!/usr/bin/env python

import numpy as np
from lsst.obs.pfs.isrTask import PfsIsrTask as IsrTask


class IPCTask(IsrTask):
    @classmethod
    def bootstrap(cls):
        config = IsrTask.ConfigClass()
        config.doBias = False
        config.doDark = False
        config.doFringe = False
        config.doFlat = False
        config.doLinearize = False
        config.doDefect = True
        config.doIPC = True
        config.ipcCoeffs = np.array([13e-3, 6e-3])
        config.doSaturationInterpolation = False
        config.validate()
        return cls(config=config)
