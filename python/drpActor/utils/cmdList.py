import drpActor.utils.cmd as drpCmd


class Ingest(drpCmd.DrpCommand):
    """ Placeholder for ingest command. """
    cmdHead = 'ingestPfsImages'

    def __init__(self, engine, files):
        """Enable string interpolation for config file

        Parameters
        ----------
        target : `str`
           Path to data repository.
        files : list of `PfsFile`
           Exposure file path.
        pfsConfigDir : `str`
           Directory with pfsConfig/pfsDesign files.
        mode : `str`
           Mode of delivering the files to their destination.
        """
        filepath = [file.filepath for file in files]
        cmdArgs = f"{' '.join(filepath)} --processes {engine.nProcesses} --pfsConfigDir {engine.pfsConfigDir} --mode={engine.ingestMode}"
        # config = dict(clobber=True, register=dict(ignore=True))
        config = dict()
        drpCmd.DrpCommand.__init__(self, engine.target, cmdArgs, config=config)


class IngestPgsql(Ingest):
    """ Placeholder for ingest command. """
    cmdHead = 'ingestPfsImagesPgsql'


class Detrend(drpCmd.DrpCommand):
    """ Placeholder for Detrend command. """
    cmdHead = 'detrend'

    def __init__(self, engine, visit, windowed=False):
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
        cmdArgs = f"--processes {engine.nProcesses} --calib  {engine.target}/{engine.CALIB} --rerun {engine.rerun} " \
                  f"--id visit={visit} --clobber-config --no-versions"

        # we want probably to this from yaml/pfs_instdata but has not been merged yet
        isr = dict(doBias=True,
                   doDark=True,
                   doFlat=False,
                   doDefect=False,
                   windowed=windowed)
        repair = dict(doCosmicRay=False)
        config = dict(isr=isr, repair=repair)

        drpCmd.DrpCommand.__init__(self, engine.target, cmdArgs, config=config)


class ReduceExposure(drpCmd.DrpCommand):
    """ Placeholder for ReduceExposure command. """
    cmdHead = 'reduceExposure'

    def __init__(self, engine, visit, windowed=False):
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
        cmdArgs = f"--processes {engine.nProcesses} --calib {engine.target}/{engine.CALIB} --rerun {engine.rerun} " \
                  f"--id visit={visit} --clobber-config --no-versions"

        # we want probably to this from yaml/pfs_instdata but has not been merged yet
        isr = dict(doFlat=False, doSaturationInterpolation=False)
        repair = dict(doCosmicRay=False)
        extractSpectra = dict(doCrosstalk=True)
        config = dict(isr=isr, repair=repair, windowed=windowed, extractSpectra=extractSpectra,
                      doAdjustDetectorMap=False, doMeasureLines=False)

        drpCmd.DrpCommand.__init__(self, engine.target, cmdArgs, config=config)
