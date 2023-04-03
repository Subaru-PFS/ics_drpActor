#!/usr/bin/env python

from lsst.obs.pfs.ingest import PfsIngestTask, PfsPgsqlIngestTask


class IngestTask(PfsIngestTask):
    def __init__(self, tasksExec, parsedCmd):
        self.tasksExec = tasksExec
        self.parsedCmd = parsedCmd

        PfsIngestTask.__init__(self, config=parsedCmd.config, log=tasksExec.engine.actor.logger)

    @property
    def engine(self):
        return self.tasksExec.engine

    @classmethod
    def bootstrap(cls, tasksExec):
        """Parse the command-line arguments and run the Task."""
        config = cls.ConfigClass()
        parser = cls.ArgumentParser(name=cls._DefaultName)
        parsedCmd = parser.parse_args(config, args=[tasksExec.engine.target, 'filepath.fits'])
        return cls(tasksExec, parsedCmd)

    def runFromActor(self, file):
        """"""
        # setting arguments
        self.parsedCmd.files = [file.filepath]
        self.parsedCmd.input = self.engine.target
        self.parsedCmd.pfsConfigDir = self.engine.pfsConfigDir
        self.parsedCmd.mode = self.engine.ingestMode

        return self.run(self.parsedCmd)


class PgsqlIngestTask(IngestTask, PfsPgsqlIngestTask):
    def __init__(self, tasksExec, parsedCmd):
        self.tasksExec = tasksExec
        self.parsedCmd = parsedCmd

        PfsPgsqlIngestTask.__init__(self, config=parsedCmd.config)
