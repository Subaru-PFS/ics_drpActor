#!/usr/bin/env python
import os
from functools import partial

from lsst.obs.pfs.detrendTask import DetrendTask as BaseTask


def toto(parsedCmd):
    print('parsing')
    taskRunner = DetrendTask.RunnerClass(TaskClass=BaseTask, parsedCmd=parsedCmd, doReturnResults=False)
    return taskRunner.run(parsedCmd)


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
                  f"--id visit=1 arm=b spectrograph=1 --clobber-config --no-versions"

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

    def runFromActor(self, file, windowed=False):
        """"""
        self.parsedCmd.config = self.ConfigClass()

        for dataId in self.parsedCmd.id.idList:
            dataId.update(**file.dataId)

        parser = self._makeArgumentParser()
        parser._applyInitialOverrides(self.parsedCmd)
        parser._processDataIds(self.parsedCmd)

        userConfig = self.tasksBox.engine.config['detrend']
        userConfig['isr'].update(windowed=windowed)
        self.mergeConfig(userConfig)

        future = self.tasksBox.executor.submit(toto, self.parsedCmd)
        future.add_done_callback(partial(self.tasksBox.detrendDoneCB, file))
