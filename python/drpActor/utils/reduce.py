import logging
import os
import time


def doReduceExposure(target, CALIB, rerun, visit, windowed=False):
    logger = logging.getLogger('reduceExposure')
    configIsr = dict(doFlat=False)
    config = dict(windowed=True)  #we want to be quick, force windowed !!

    cmd = f"reduceExposure.py {target} -j 2 --calib  {target}/{CALIB} --rerun {rerun} --id visit={visit}"
    isr = ' '.join(['-c'] + [f'isr.{key}={str(val)}' for key, val in configIsr.items()])
    cfg = ' '.join([f'{key}={str(val)}' for key, val in config.items()])

    # adding clobber because we are alterning between windowed and non windowed
    cmdStr = ' '.join([cmd, isr, cfg, '--clobber-config', '--clobber-versions'])

    logger.info(cmdStr)
    t0 = time.time()
    os.system(cmdStr)
    t1 = time.time()
    logger.info(f'reduceExposure took {t1 - t0:0.4f}s: {cmd}')
