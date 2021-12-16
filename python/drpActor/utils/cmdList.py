import drpActor.utils.cmd as drpCmd

nProcesses = 2


class Ingest(drpCmd.DrpCommand):
    """ Placeholder for ingest command. """
    cmdHead = 'ingestPfsImages'

    def __init__(self, target, filepath, pfsConfigDir, mode='link'):
        """Enable string interpolation for config file

        Parameters
        ----------
        target : `str`
           Path to data repository.
        filepath : `str`
           Exposure file path.
        pfsConfigDir : `str`
           Directory with pfsConfig/pfsDesign files.
        mode : `str`
           Mode of delivering the files to their destination.
        """

        cmdArgs = f"{filepath} --processes {nProcesses} --pfsConfigDir {pfsConfigDir} --mode={mode}"
        # config = dict(clobber=True, register=dict(ignore=True))
        config = dict()
        drpCmd.DrpCommand.__init__(self, target, cmdArgs, config=config)


class Detrend(drpCmd.DrpCommand):
    """ Placeholder for Detrend command. """
    nProcesses = 2
    cmdHead = 'detrend'

    def __init__(self, target, CALIB, rerun, visit, windowed=False):
        """Perform ISR on given exposure.

        Parameters
        ----------
        target : `str`
           Path to data repository.
        CALIB : `str`
           Path to input calibration repository.
        rerun : `str`
           rerun name.
        visit : `int`
           PFS visit.
        """
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
    """ Placeholder for ReduceExposure command. """
    cmdHead = 'reduceExposure'

    def __init__(self, target, CALIB, rerun, visit, windowed=False):
        """Reduce spectra from given exposure.

        Parameters
        ----------
        target : `str`
           Path to data repository.
        CALIB : `str`
           Path to input calibration repository.
        rerun : `str`
           rerun name.
        visit : `int`
           PFS visit.
        """
        cmdArgs = f"--processes {nProcesses} --calib {target}/{CALIB} --rerun {rerun} " \
                  f"--id visit={visit} --clobber-config --no-versions"

        # we want probably to this from yaml/pfs_instdata but has not been merged yet
        isr = dict(doFlat=False)
        repair = dict()
        config = dict(isr=isr, repair=repair, windowed=True)  # we want to be quick, force windowed !!
        drpCmd.DrpCommand.__init__(self, target, cmdArgs, config=config)
