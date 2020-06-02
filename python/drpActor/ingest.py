from lsst.obs.pfs.ingest import PfsIngestTask


def doIngest(filepath, target):
    """example code from Craig"""
    ingestTask = PfsIngestTask()
    config = ingestTask.ConfigClass()
    parser = ingestTask.ArgumentParser(name="ingest", )
    args = parser.parse_args(config,
                             args=[target, '--pfsConfigDir', '/data/drp/pfsDesign', '--mode=link', filepath, '-c',
                                   'clobber=True'])
    task = PfsIngestTask(config=args.config)
    task.run(args)