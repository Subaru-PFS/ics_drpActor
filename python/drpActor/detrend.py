from lsst.obs.pfs.detrendTask import DetrendTask


def doDetrend(visit, target, rerun, arm=None):
    """example code from Craig"""
    args = [target]
    args.extend(('--calib', '%s/CALIB' % target))
    args.extend(('--rerun', '%s/detrend' % rerun))
    args.extend(('--id', 'visit=%d' % visit))
    if arm is not None:
        args.append('arm=%s' % arm)
    # args.append('--doraise')
    args.append('--clobber-versions')

    config = DetrendTask.ConfigClass()
    config.isr.doBias = True
    config.isr.doDark = True
    config.isr.doFlat = False
    config.isr.doDefect = False # Until PIPE2D-715 is fixed.

    # things we probably want to turn on

    # config.isr.doLinearize = False
    # config.isr.doFringe = False
    #
    # config.isr.doSaturationInterpolation = False
    # config.repair.doInterpolate = False
    config.repair.cosmicray.keepCRs = True
    config.repair.cosmicray.nCrPixelMax = 250000

    detrendTask = DetrendTask()
    cooked = detrendTask.parseAndRun(config=config, args=args)

    return cooked

# python -c "
# from lsst.daf.persistence import Butler
# butler = Butler("/drp/lam/rerun/pfs/detrend")
# arc = butler.get(\"pfsArm\", visit=5825, arm=\"r\", spectrograph=1)
