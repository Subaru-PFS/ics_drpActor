#!/usr/bin/env python
import os
from functools import partial

from pfs.drp.qa.detectorMapQa import DetectorMapQaTask as BaseTask


def runOutsideClass(parsedCmd):
    taskRunner = DetectorMapQaTask.RunnerClass(TaskClass=BaseTask, parsedCmd=parsedCmd, doReturnResults=False)
    return taskRunner.run(parsedCmd)


class DetectorMapQaTask(BaseTask):
    def __init__(self, tasksExec, parsedCmd):
        self.tasksExec = tasksExec
        self.parsedCmd = parsedCmd

        BaseTask.__init__(self, config=parsedCmd.config)

    @property
    def engine(self):
        return self.tasksExec.engine

    @classmethod
    def bootstrap(cls, tasksExec, visit=1, arm='b', spectrograph=1):
        """Parse the command-line arguments and run the Task."""
        config = cls.ConfigClass()
        parser = cls._makeArgumentParser()

        cmdArgs = tasksExec.engine.makeCmdArgs(visit, arm, spectrograph)
        parsedCmd = parser.parse_args(config, args=cmdArgs.split(' '))

        return cls(tasksExec, parsedCmd)

    def mergeConfig(self, userConfig):
        """Merging config the default overloaded config, with the yaml user config."""
        for key, cfg in userConfig.items():
            if isinstance(cfg, dict):
                getattr(self.parsedCmd.config, key).update(**cfg)
            else:
                self.parsedCmd.config.update(**{key: cfg})

        self.parsedCmd.config.validate()

    def runFromActor(self, file):
        """"""
        # resetting the frozen config file.
        self.parsedCmd.config = DetectorMapQaTask.ConfigClass()
        parser = self._makeArgumentParser()
        self.tasksExec.actor.cleanLogHandlers()  # the parser add some unwanted handlers, get rid of those.
        parser._applyInitialOverrides(self.parsedCmd)

        # updating the idList.
        for dataId in self.parsedCmd.id.idList:
            dataId.update(**file.dataId)

        # resetting the ref list.
        self.parsedCmd.id.refList = []
        self.parsedCmd.id.makeDataRefList(self.parsedCmd)

        # merging with user config.
        try:
            userConfig = self.tasksExec.engine.config['detectorMapQa']
            self.mergeConfig(userConfig)

        except Exception as e:
            self.tasksExec.engine.logger.warning(f'failed to load user config for detectorMapQa with : {str(e)}')

        # send task to the ProcessPoolExecutor.
        future = self.tasksExec.executor.submit(runOutsideClass, self.parsedCmd)
        future.add_done_callback(partial(self.tasksExec.waitForDetectorMapQa, file))
