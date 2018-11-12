import subprocess


def doDetrend(visit, rerun='ginga', target="/drp/lam", cmdlineArgs=None):
    RAW_DATA_DIR = "/data/pfs/pfs"

    conf = {'isr.doFlat': False,
            'isr.doDark': False,
            'repair.cosmicray.keepCRs': False,
            }
    # batchArgs2="-c repair.cosmicray.nCrPixelMax=100000"

    batchArgs = [target, '--calib', '%s/CALIB' % target, '--rerun', '%s/detrend' % rerun, '--id', 'visit=%d' % visit,
                 "--clobber-versions"]

    for key, value in conf.items():
        batchArgs += ['-c', '%s=%s' % (key, value)]

    return subprocess.call(['detrend.py'] + batchArgs)