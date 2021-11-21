import logging
import os
import time


def doIngest(filepath, target, pfsConfigDir, mode='link'):
    logger = logging.getLogger('ingest')
    cmd = f'ingestPfsImages.py {target} --pfsConfigDir {pfsConfigDir} --config clobber=True register.ignore=True --mode={mode} {filepath}'
    t0 = time.time()
    os.system(cmd)
    t1 = time.time()
    logger.info(f'ingest took {t1 - t0:0.4f}s: {cmd}')
