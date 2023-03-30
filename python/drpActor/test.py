#!/usr/bin/env python

import sys

import lsst.log
import lsst.utils.introspection
import lsst.utils.logging
from lsst.obs.pfs.detrendTask import DetrendTask as BaseTask
from lsst.pipe.base.struct import Struct


class DetrendTask(BaseTask):

    @classmethod
    def parseAndRun(cls, args=None, config=None, log=None, doReturnResults=False):
        """"""
        if args is None:
            commandAsStr = " ".join(sys.argv)
            args = sys.argv[1:]
            print(args)
        else:
            commandAsStr = "{}{}".format(lsst.utils.introspection.get_caller_name(stacklevel=1), tuple(args))

        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        # print this message after parsing the command so the log is fully
        # configured
        parsedCmd.log.info("Running: %s", commandAsStr)

        taskRunner = cls.RunnerClass(TaskClass=cls, parsedCmd=parsedCmd, doReturnResults=doReturnResults)
        resultList = taskRunner.run(parsedCmd)

        try:
            nFailed = sum(((res.exitStatus != 0) for res in resultList))
        except (TypeError, AttributeError) as e:
            # NOTE: TypeError if resultList is None, AttributeError if it
            # doesn't have exitStatus.
            parsedCmd.log.warning("Unable to retrieve exit status (%s); assuming success", e)
            nFailed = 0

        if nFailed > 0:
            if parsedCmd.noExit:
                parsedCmd.log.error("%d dataRefs failed; not exiting as --noExit was set", nFailed)
            else:
                sys.exit(nFailed)

        return Struct(
            argumentParser=argumentParser,
            parsedCmd=parsedCmd,
            taskRunner=taskRunner,
            resultList=resultList,
        )


task = DetrendTask()
task.parseAndRun()
