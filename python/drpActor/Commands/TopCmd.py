#!/usr/bin/env python

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from drpActor.utils import threaded


class TopCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.name = "drpCheck"
        self.vocab = [
            ('set', '<drpFolder>', self.changeFolder),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("drp_drp", (1, 1),
                                        keys.Key("drpFolder", types.String(), help="custom drp folder"),
                                        )

    @threaded
    def changeFolder(self, cmd):
        cmdKeys = cmd.cmd.keywords
        drpFolder = cmdKeys["drpFolder"].values[0]
        self.actor.drpFolder = drpFolder
        cmd.finish("drpFolder='%s'" % drpFolder)
