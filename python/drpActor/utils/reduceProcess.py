"""Run the reduction pipeline with SeparablePipelineExecutor in a child process, one reduction at a time."""

import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
import traceback

# one reduction at a time in the actor process, whatever the number of engines.
_lock = threading.Lock()


def reductionTimeout(nVisits, base=600, perVisit=600):
    """
    Seconds a reduction may run before it is considered hung.

    The defaults allow at least 1.5 times the longest drp2 reduction of 1 to 45 visits with numProc=16.

    Parameters
    ----------
    nVisits : int
        Number of visits in the quantum graph, 0 while the graph is being built.
    base : float
        Seconds allowed whatever the number of visits, covering the graph building.
    perVisit : float
        Seconds allowed per visit.

    Returns
    -------
    timeout : float
        Seconds.
    """
    return base + perVisit * nVisits


def runInChild(logFile, timeout=None, pollInterval=10, **reduceArgs):
    """
    Run a reduction in a child process, waiting for its outcome.

    The child runs in its own process group, so that killing it also kills the quanta it spawned. Its stdout and
    stderr, inherited by the quanta processes, go to logFile.

    Parameters
    ----------
    logFile : str
        Path of the child's log file, created with its directory.
    timeout : dict, optional
        Keyword arguments of reductionTimeout, the child being killed once the reduction runs longer than its
        visits allow, None to never kill it.
    pollInterval : float
        Seconds between checks of the child.
    **reduceArgs
        Arguments of reduce.

    Returns
    -------
    status : str
        'ok', 'failed', 'conflict' (the run holds other software versions or task configs), 'timeout' or
        'crashed' (the child exited without an outcome).
    message : str
        Details of a failure, empty on success.
    """
    os.makedirs(os.path.dirname(logFile), exist_ok=True)
    context = multiprocessing.get_context('spawn')

    with _lock:
        receiver, sender = context.Pipe(duplex=False)
        # not a daemon, a daemonic process cannot start the quanta processes.
        child = context.Process(target=_childMain, kwargs=dict(conn=sender, logFile=logFile, **reduceArgs),
                                daemon=False)
        start = time.monotonic()
        child.start()
        sender.close()

        limit = None if timeout is None else reductionTimeout(0, **timeout)

        while True:
            if receiver.poll(pollInterval):
                try:
                    status, message = receiver.recv()
                except EOFError:
                    child.join()
                    return 'crashed', f'child exited with code {child.exitcode} without an outcome'

                # the number of visits in the quantum graph, the reduction is still running.
                if status == 'graph':
                    if timeout is not None:
                        limit = reductionTimeout(message, **timeout)
                    continue

                child.join()
                return status, message

            if not child.is_alive():
                child.join()
                return 'crashed', f'child exited with code {child.exitcode} without an outcome'

            if limit is not None and time.monotonic() - start > limit:
                _kill(child)
                return 'timeout', f'reduction killed after {limit:.0f}s'


def _kill(child):
    """Kill the child and the processes it started, then reap it."""
    # the child may have died before becoming its own process group leader.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    child.kill()
    child.join()


def _childMain(conn, logFile, **reduceArgs):
    """Child process entry point: run reduce, logging to logFile, and send back its outcome."""
    os.setsid()

    # file descriptors rather than logging handlers, so that the quanta processes inherit them.
    fd = os.open(logFile, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o664)
    os.dup2(fd, sys.stdout.fileno())
    os.dup2(fd, sys.stderr.fileno())
    os.close(fd)

    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter('%(levelname)s %(asctime)s %(name)s - %(message)s'))
        root.addHandler(handler)
    root.setLevel(logging.INFO)

    from lsst.daf.butler.registry import ConflictingDefinitionError

    try:
        reduce(**reduceArgs, onGraph=lambda nVisits: conn.send(('graph', nVisits)))
        outcome = ('ok', '')
    except ConflictingDefinitionError as e:
        outcome = ('conflict', str(e))
    except Exception:
        logging.getLogger('drpActor.reduceProcess').exception('reduction failed')
        outcome = ('failed', traceback.format_exc(limit=-3))

    conn.send(outcome)
    conn.close()


def graphVisits(quantumGraph):
    """
    Number of visits in a quantum graph.

    Parameters
    ----------
    quantumGraph : lsst.pipe.base.QuantumGraph
        Quantum graph.

    Returns
    -------
    nVisits : int
        Number of distinct visits in the data IDs of the quanta, quanta without a visit being ignored.
    """
    visits = set()

    for node in quantumGraph:
        dataId = node.quantum.dataId
        if 'visit' in dataId.dimensions.names:
            visits.add(dataId['visit'])

    return len(visits)


def reduce(datastore, inputCollection, run, pipelinePath, where, configOverride=None, numProc=1, taskThreads=1,
           failFast=False, clobberOutput=True, onGraph=None):
    """
    Run the reduction pipeline on the selected data with SeparablePipelineExecutor, in the current process.

    Parameters
    ----------
    datastore : str
        Butler repo root/URI.
    inputCollection : str
        Input collection.
    run : str
        Output RUN collection.
    pipelinePath : str
        Full path to the pipeline YAML.
    where : str
        Data query selecting what to reduce.
    configOverride : dict, optional
        {taskLabel: {configKey: value}}, labels absent from the pipeline are ignored.
    numProc : int
        Number of processes running quanta.
    taskThreads : int
        Cores per quantum.
    failFast : bool
        Stop at the first failing quantum.
    clobberOutput : bool
        Replace existing outputs of the reduced data in the run.
    onGraph : callable, optional
        Called with the number of visits in the quantum graph, before the graph is run.

    Raises
    ------
    lsst.daf.butler.registry.ConflictingDefinitionError
        If the run holds other software versions or task configs.
    """
    from lsst.daf.butler import Butler
    from lsst.pipe.base import ExecutionResources, Pipeline
    from lsst.pipe.base.separable_pipeline_executor import SeparablePipelineExecutor

    pipeline = Pipeline.fromFile(pipelinePath)

    for label, cfg in (configOverride or {}).items():
        if label not in pipeline.task_labels:
            continue
        for key, value in cfg.items():
            pipeline.addConfigOverride(label, key=key, value=value)

    butler = Butler(datastore, collections=[inputCollection], run=run)
    executor = SeparablePipelineExecutor(butler=butler, clobber_output=clobberOutput,
                                         resources=ExecutionResources(num_cores=taskThreads))

    quantumGraph = executor.make_quantum_graph(pipeline=pipeline, where=where)

    if onGraph is not None:
        onGraph(graphVisits(quantumGraph))

    executor.pre_execute_qgraph(quantumGraph)
    executor.run_pipeline(graph=quantumGraph, num_proc=numProc, fail_fast=failFast)
