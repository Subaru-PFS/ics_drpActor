"""Run the reduction pipeline in a pipetask subprocess."""

import logging
import os
import subprocess


def pipetaskCommand(datastore, inputCollection, run, pipelineYaml, where, configOverride=None, numProc=1,
                    taskThreads=1, failFast=False, clobberOutput=False, logLevel=logging.INFO, longLog=True):
    """
    Build the pipetask command line extending an existing output run.

    Parameters
    ----------
    datastore : str
        Butler repo root/URI.
    inputCollection : str
        Input collection.
    run : str
        Existing output RUN collection, extended.
    pipelineYaml : str
        Full path to the pipeline YAML.
    where : str
        Data query selecting what to reduce.
    configOverride : dict, optional
        {taskLabel: {configKey: value}}.
    numProc : int
        Number of worker processes (pipetask -j).
    taskThreads : int
        Cores per quantum (pipetask -n).
    failFast : bool
        Stop at the first failing quantum.
    clobberOutput : bool
        Remove outputs of failed quanta that would block new quanta.
    logLevel : int
        Logging level of the pipetask loggers.
    longLog : bool
        Use the long log format.

    Returns
    -------
    cmd : list of str
        The command, to be run without a shell.
    """
    cmd = ['pipetask']
    if longLog:
        cmd.append('--long-log')
    cmd += ['--log-level', f'.={logging.getLevelName(logLevel)}', 'run',
            '-b', datastore, '-i', inputCollection, '--output-run', run, '--extend-run',
            '-p', pipelineYaml, '-d', where, '-j', str(numProc), '-n', str(taskThreads)]

    for label, cfg in (configOverride or {}).items():
        for key, value in cfg.items():
            cmd += ['-c', f'{label}:{key}={value}']

    if failFast:
        cmd.append('--fail-fast')
    if clobberOutput:
        cmd.append('--clobber-outputs')

    return cmd


def runPipetask(cmd, logFile):
    """
    Run a pipetask command, writing its output to a log file.

    Parameters
    ----------
    cmd : list of str
        Command, see pipetaskCommand.
    logFile : str
        Path of the log file, created with its directory.

    Returns
    -------
    returnCode : int
        pipetask exit code, 0 on success.
    """
    os.makedirs(os.path.dirname(logFile), exist_ok=True)

    with open(logFile, 'w') as log:
        log.write(' '.join(cmd) + '\n\n')
        log.flush()
        return subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode


def runIsInconsistent(logFile):
    """
    Whether pipetask failed because the run holds other software versions or task configs.

    Parameters
    ----------
    logFile : str
        pipetask log file, see runPipetask.

    Returns
    -------
    inconsistent : bool
        True if pipetask raised ConflictingDefinitionError.
    """
    with open(logFile) as log:
        return any('ConflictingDefinitionError' in line for line in log)
