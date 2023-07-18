#!/usr/bin/env python

import logging
from functools import partial

import numpy as np
from lsst.obs.pfs.isrTask import PfsIsrTask as IsrTask


def runOutsideClass(dataId):
    """Doing IPC task, note that this is called from multiprocessing, there isn't access to the actual tasksExec
    instance."""
    # raw image is actually :  hdu[-1] - hdu[0], both reference pixel subtracted, reset frame is ignored.
    cds = tasksExec.butler.get('raw', dataId=dataId)
    calibs = tasksExec.getCalibs(dataId)

    logging.info(f'running doIPCTask for {dataId}')
    calexp = tasksExec.ipcTask.run(cds, defects=calibs['defects'], flat=calibs['flat']).exposure

    logging.info(f'doIPCTask done, putting output to butler.')
    tasksExec.butler.put(calexp, 'calexp', dataId)


class IPCTask(IsrTask):

    def __init__(self, tasksExec, *args, **kwargs):
        self.tasksExec = tasksExec
        IsrTask.__init__(self, *args, **kwargs)

    @classmethod
    def bootstrap(cls, tasksExec):
        config = IsrTask.ConfigClass()
        config.doBias = False
        config.doDark = False
        config.doFringe = False
        config.doFlat = False
        config.doLinearize = False
        config.doDefect = True
        config.doIPC = True
        #config.ipcCoeffs = np.array([13e-3, 6e-3])
        config.doSaturationInterpolation = False
        config.validate()

        return cls(tasksExec, config=config)

    def runFromActor(self, file, **option):
        """"""
        global tasksExec
        tasksExec = self.tasksExec
        # calling multiprocessing to do IPCTask.
        future = self.tasksExec.executor.submit(runOutsideClass, file.dataId)
        future.add_done_callback(partial(self.tasksExec.waitForCalexp, file))
