import os
from lsst.pipe.tasks.ingest import IngestTask


class MyIngestTask(IngestTask):
    armNum = {'1': 'b',
              '2': 'r',
              '3': 'n',
              '4': 'm'}

    def __init__(self, *args, **kwargs):
        super(MyIngestTask, self).__init__(*args, **kwargs)

    @classmethod
    def customIngest(cls, filepath, target):
        ingestTask = IngestTask()
        config = ingestTask.ConfigClass()
        parser = ingestTask.ArgumentParser(name="ingest", )
        args = parser.parse_args(config,
                                 args=[target, "--mode=link", filepath, "-c", "clobber=True", "register.ignore=True"])
        task = cls(config=args.config)
        task.run(args)
