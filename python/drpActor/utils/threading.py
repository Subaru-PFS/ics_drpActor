import time
from functools import partial

from actorcore.QThread import QThread


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
        self.useSilentThread = self.engine.actor.actorConfig['genDetrendKey'].get('useSilentThread', False)

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
