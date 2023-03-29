#!/usr/bin/env python
import os

from lsst.obs.pfs.detrendTask import DetrendTask as BaseTask


class DetrendTask(BaseTask):
    def __init__(self, tasksBox, parsedCmd):
        self.tasksBox = tasksBox
        self.parsedCmd = parsedCmd

        BaseTask.__init__(self, config=parsedCmd.config)

    @property
    def engine(self):
        return self.tasksBox.engine

    @classmethod
    def bootstrap(cls, tasksBox):
        """Parse the command-line arguments and run the Task."""
        config = cls.ConfigClass()

        CALIB = os.path.join(tasksBox.engine.target, tasksBox.engine.CALIB)
        cmdArgs = f"{tasksBox.engine.target} --processes 1 --calib {CALIB} --rerun {tasksBox.engine.rerun} " \
                  f"--id visit=1 arm=b^r --clobber-config --no-versions"

        parser = cls._makeArgumentParser()
        parsedCmd = parser.parse_args(config, args=cmdArgs.split(' '))

        return cls(tasksBox, parsedCmd)

    def mergeConfig(self, userConfig):
        # merging config.
        for key, cfg in userConfig.items():
            if isinstance(cfg, dict):
                getattr(self.parsedCmd.config, key).update(**cfg)
            else:
                self.parsedCmd.config.update(key=cfg)

        self.parsedCmd.config.validate()

    def runFromActor(self, visit, nProcesses, windowed=False):
        """"""
        self.parsedCmd.config = self.ConfigClass()
        self.parsedCmd.processes = nProcesses

        for dataId in self.parsedCmd.id.idList:
            dataId.update(visit=visit)

        parser = self._makeArgumentParser()
        parser._applyInitialOverrides(self.parsedCmd)
        parser._processDataIds(self.parsedCmd)

        userConfig = self.tasksBox.engine.config['detrend']
        userConfig['isr'].update(windowed=windowed)
        self.mergeConfig(userConfig)

        taskRunner = DetrendTask.RunnerClass(TaskClass=BaseTask, parsedCmd=self.parsedCmd, doReturnResults=False)

        return taskRunner.run(self.parsedCmd)
