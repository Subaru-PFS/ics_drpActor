import os
from lsst.obs.pfs.ingest import PfsIngestTask


class MyIngestTask(PfsIngestTask):
    armNum = {'1': 'b',
              '2': 'r',
              '3': 'n',
              '4': 'm'}

    def __init__(self, *args, **kwargs):
        super(MyIngestTask, self).__init__(*args, **kwargs)

    @classmethod
    def customIngest(cls, filepath, target):
        ingestTask = PfsIngestTask()
        config = ingestTask.ConfigClass()
        parser = ingestTask.ArgumentParser(name="ingest", )
        args = parser.parse_args(config,args=[target, '--pfsConfigDir', '/data/drp/pfsDesign', '--mode=link', filepath, '-c', 'clobber=True'])
        task = cls(config=args.config)
        task.run(args)