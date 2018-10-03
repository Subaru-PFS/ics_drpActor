from lsst.pipe.tasks.ingest import IngestTask


class MyIngestTask(IngestTask):
    def __init__(self, *args, **kwargs):
        super(MyIngestTask, self).__init__(*args, **kwargs)

    @classmethod
    def customIngest(cls, path):
        ingestTask = IngestTask()
        config = ingestTask.ConfigClass()
        parser = ingestTask.ArgumentParser(name="ingest", )
        args = parser.parse_args(config, args=['/drp/lam', "--mode=link", path, "-c", "clobber=True", "register.ignore=True"])
        task = cls(config=args.config)
        task.run(args)
