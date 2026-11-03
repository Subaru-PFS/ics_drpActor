import time
from functools import partial, wraps

from actorcore.QThread import QMsg, QThread


class Silence:
    """
    Helper class to handle silent execution of broadcast messages.

    Parameters
    ----------
    engine : object
        The engine instance managing the thread and configurations.
    """

    def __init__(self, engine):
        self.engine = engine
        self.useSilentThread = self.engine.detrendCallback.get('useSilentThread', False)

    def diag(self, *args, **kwargs):
        """
        Send a diagnostic message if silent mode is not enabled.

        Parameters
        ----------
        *args, **kwargs :
            Arguments to pass to the diagnostic broadcast function.
        """
        if self.useSilentThread:
            return  # Skip broadcast if silent mode is enabled.

        return self.engine.actor.bcast.diag(*args, **kwargs)

    def warn(self, *args, **kwargs):
        """
        Send a warning message through broadcast.

        Parameters
        ----------
        *args, **kwargs :
            Arguments to pass to the warning broadcast function.
        """
        return self.engine.actor.bcast.warn(*args, **kwargs)


class SilentThread(QThread):
    """
    A custom thread class that runs tasks silently, allowing optional command injection.

    Parameters
    ----------
    engine : object
        The engine instance managing this thread.

    Attributes
    ----------
    exitASAP : bool
        A flag indicating that the thread should exit as soon as possible after executing the task.
    """

    def __init__(self, engine):
        self.engine = engine
        # Initialize the parent QThread with a unique identifier (timestamp).
        super().__init__(engine.actor, str(time.time()))

    def _realCmd(self, cmd=None):
        """
        Retrieve a callable command instance. If no command is provided, return a Silence object.

        Parameters
        ----------
        cmd : callable, optional
            The command instance to use (default: None).

        Returns
        -------
        callable
            The provided command instance or a Silence object if no command is passed.
        """
        return cmd if cmd else Silence(self.engine)


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


def singleShot(func):
    """
    Decorator to run a function in a separate thread and ensure it exits upon completion.

    The function will be executed within a `SilentThread`, which is destroyed as soon
    as the task is finished. This avoids blocking the main thread and ensures cleanup.

    Parameters
    ----------
    func : callable
        The function to be executed within a separate thread.

    Returns
    -------
    callable
        A wrapper function that manages thread creation and task execution.
    """

    def wrapper(self, engine, *args, **kwargs):
        """
        Create and start a `SilentThread` to run the decorated function.

        Parameters
        ----------
        self : object
            The instance of the class containing the decorated method.
        engine : object
            The engine instance required to manage the thread.
        *args, **kwargs :
            Arguments to pass to the decorated function.
        """
        # Initialize and start a new SilentThread.
        thread = SilentThread(engine)
        thread.start()

        # Schedule the function to be executed within the thread.
        thread.putMsg(partial(func, self, engine, *args, **kwargs))

        # Mark the thread for exit after the function completes.
        thread.exitASAP = True

    return wrapper
