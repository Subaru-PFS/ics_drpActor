#!/usr/bin/env python


from drpActor.utils.tasks.detrend import DetrendTask
from lsst.obs.pfs.detrendTask import DetrendTask as BaseTask


class Engine(object):
    def __init__(self):
        self.target = '/data/drp/sps'
        self.rerun = 'ginga/detrend'
        self.CALIB = 'CALIB'
        self.pfsConfigDir = '/data/raw'
        self.nProcesses = 4


class TasksBox(object):
    def __init__(self):
        self.engine = Engine()

        self.detrendTask = DetrendTask.bootstrap(self)


tasks = TasksBox()
tasks.detrendTask.runFromActor(48764, 1)


# parsedCmd = tasks.detrendTask.parsedCmd
# taskRunner = DetrendTask.RunnerClass(TaskClass=BaseTask, parsedCmd=parsedCmd, doReturnResults=False)
#
