
from lsst.obs.pfs.detrendTask import DetrendTask


def detrend(visit, rerun, site=None, cam=None, cmdlineArgs=None):
    """example code from Craig"""
    arm = cam[0]
    spec = int(cam[1])

    args = [dataRoot(site)]
    args.append('--rerun=%s' % (rerun))
    args.append('--doraise')
    args.append('--clobber-versions')
    args.extend(('--id', 'visit=%s' % (visit)))
    if cmdlineArgs is not None:
        if isinstance(cmdlineArgs, str):
            import shlex
            cmdlineArgs = shlex.split(cmdlineArgs)
        args.extend(cmdlineArgs)

    config = DetrendTask.ConfigClass()
    config.isr.doBias = True
    config.isr.doDark = True

    # things we probably want to turn on
    config.isr.doFlat = False
    config.isr.doLinearize = False
    config.isr.doFringe = False

    config.isr.doSaturationInterpolation = False
    config.repair.doInterpolate = False
    config.repair.cosmicray.keepCRs = True

    detrendTask = DetrendTask()
    print("args: ", args)
    cooked = detrendTask.parseAndRun(config=config, args=args)

    return cooked




#python -c "
#from lsst.daf.persistence import Butler
#butler = Butler("/drp/lam/rerun/pfs/detrend")
#arc = butler.get(\"pfsArm\", visit=5825, arm=\"r\", spectrograph=1)
