#!/usr/bin/env python

from lsst.obs.pfs.ingest import PfsIngestTask, PfsPgsqlIngestTask


class IngestTask(PfsIngestTask):
    def __init__(self, tasksBox, parsedCmd):
        self.tasksBox = tasksBox
        self.parsedCmd = parsedCmd

        PfsIngestTask.__init__(self, config=parsedCmd.config, log=tasksBox.engine.actor.logger)

    @property
    def engine(self):
        return self.tasksBox.engine

    @classmethod
    def bootstrap(cls, tasksBox):
        """Parse the command-line arguments and run the Task."""
        config = cls.ConfigClass()
        parser = cls.ArgumentParser(name=cls._DefaultName)
        parsedCmd = parser.parse_args(config, args=[tasksBox.engine.target, 'filepath.fits'])
        return cls(tasksBox, parsedCmd)

    def runFromActor(self, files):
        """"""
        # setting arguments
        self.parsedCmd.files = files
        self.parsedCmd.input = self.engine.target
        self.parsedCmd.pfsConfigDir = self.engine.pfsConfigDir
        self.parsedCmd.mode = self.engine.ingestMode

        return self.run(self.parsedCmd)


class PgsqlIngestTask(IngestTask, PfsPgsqlIngestTask):
    def __init__(self, tasksBox, parsedCmd):
        self.tasksBox = tasksBox
        self.parsedCmd = parsedCmd

        PfsPgsqlIngestTask.__init__(self, config=parsedCmd.config)
