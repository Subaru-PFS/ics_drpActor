from functools import wraps

from actorcore.QThread import QMsg, QThread


class LogReplies:
    """Stand-in for a command, writing the thread's own diag and warn replies to the actor log."""

    def __init__(self, logger):
        self.logger = logger

    def diag(self, text):
        self.logger.debug(text)

    def warn(self, text):
        self.logger.warning(text)


class Worker(QThread):
    """
    Thread executing the messages put on its queue one at a time, in the order they were put, started on creation.

    The thread's own replies go to the actor log, so it can be started before the actor's bcast exists.

    Parameters
    ----------
    actor : actorcore.Actor
        Actor whose logger records the thread's replies and uncaught exceptions.
    name : str
        Thread name.
    """

    def __init__(self, actor, name):
        super().__init__(actor, name, timeout=60)
        self.start()

    def _realCmd(self, cmd=None):
        """Return cmd, or a LogReplies on the actor log."""
        return cmd if cmd else LogReplies(self.actor.logger)

    def handleTimeout(self):
        """Stay silent while idle, exit when asked to."""
        if self.exitASAP:
            raise SystemExit()

    def logExceptions(self, method):
        """
        Wrap method so that an exception it raises is logged with its traceback before propagating.

        A command passed to method as its cmd argument is failed, unless already finished or the actor's bcast.
        """

        @wraps(method)
        def wrapper(**argd):
            try:
                return method(**argd)
            except Exception as e:
                name = getattr(method, "__name__", method)
                self.actor.logger.exception(f'{self.name} thread: exception in {name}')

                cmd = argd.get('cmd')
                if cmd is not None and cmd is not getattr(self.actor, 'bcast', None) and cmd.alive:
                    reason = str(e).replace('"', "'")
                    cmd.fail(f'text="{name} failed: {reason}"')

                raise

        return wrapper

    def putMsg(self, method, urgent=False, **argd):
        """
        Put a message on the queue.

        Parameters
        ----------
        method : callable
            Method to call, with the keyword arguments argd.
        urgent : bool
            Put the message at the front of the queue.
        """
        super().putMsg(self.logExceptions(method), urgent=urgent, **argd)

    def putMsgLater(self, delay, method, **argd):
        """
        Put a message on the queue after a delay.

        Parameters
        ----------
        delay : float
            Seconds before the message is queued.
        method : callable
            Method to call, with the keyword arguments argd.
        """
        self.sendLater(QMsg(self.logExceptions(method), **argd), delay)
