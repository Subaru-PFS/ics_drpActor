import drpActor.utils.cmd as drpCmd

nProcesses = 2


class Ingest(drpCmd.DrpCommand):
    cmdHead = 'ingestPfsImages'

    def __init__(self, target, filepath, pfsConfigDir, mode='link'):
        """"""
        cmdArgs = f"{filepath} --processes {nProcesses} --pfsConfigDir {pfsConfigDir} --mode={mode}"
        # config = dict(clobber=True, register=dict(ignore=True))
        config = dict()
        drpCmd.DrpCommand.__init__(self, target, cmdArgs, config=config)


class Detrend(drpCmd.DrpCommand):
    nProcesses = 2
    cmdHead = 'detrend'

    def __init__(self, target, CALIB, rerun, visit, windowed=False):
        """ """
        cmdArgs = f"--processes {nProcesses} --calib  {target}/{CALIB} --rerun {rerun} " \
                  f"--id visit={visit} --clobber-config --no-versions"

        # we want probably to this from yaml/pfs_instdata but has not been merged yet
        isr = dict(doBias=True,
                   doDark=True,
                   doFlat=False,
                   doDefect=False,
                   windowed=windowed)
        repair = dict(doCosmicRay=False)
        config = dict(isr=isr, repair=repair)

        drpCmd.DrpCommand.__init__(self, target, cmdArgs, config=config)


class ReduceExposure(drpCmd.DrpCommand):
    cmdHead = 'reduceExposure'

    def __init__(self, target, CALIB, rerun, visit, windowed=False):
        """ """
        cmdArgs = f"--processes {nProcesses} --calib {target}/{CALIB} --rerun {rerun} " \
                  f"--id visit={visit} --clobber-config --no-versions"

        # we want probably to this from yaml/pfs_instdata but has not been merged yet
        isr = dict(doFlat=False)
        repair = dict()
        config = dict(isr=isr, repair=repair, windowed=True)  # we want to be quick, force windowed !!
        drpCmd.DrpCommand.__init__(self, target, cmdArgs, config=config)
