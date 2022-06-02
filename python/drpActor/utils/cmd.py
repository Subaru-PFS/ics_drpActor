import logging
import subprocess
import time


class DrpCommand:
    """ Placeholder to handle drp commands parsing and execution. """
    cmdHead = ''

    def __init__(self, target, cmdArgs, config=None):
        self.cmdStr = self.parse(target, cmdArgs, config)

    def parse(self, target, cmdArgs, config):
        """Parse command given a target, fixed cmdArgs and config dictionary."""

        def parseConfig(config):
            def parseVal(k, v):
                """Parse key, value, parseDict if value is dict."""
                if isinstance(v, dict):
                    return parseDict(v, prefix=f'{k}.')
                else:
                    return f'{k}={str(v)}'

            def parseDict(d, prefix=''):
                """Parse each key,value in dictionary."""
                return ' '.join([parseVal(f'{prefix}{k}', v).strip() for k, v in d.items()])

            if config is None or not config:
                return []

            return ['-c', parseDict(config)]

        raw = [f'{self.cmdHead}.py', target, cmdArgs] + parseConfig(config)

        return ' '.join([arg.strip() for arg in raw])

    def run(self, cmd=None, verbose=True):
        """Run the command, return status and timing."""
        t0 = time.time()
        logger = logging.getLogger(self.cmdHead)
        cmd = self.cmdStr if cmd is None else cmd

        logger.info(cmd)
        # automatically convert to str instead of bytes.
        ret = subprocess.run(cmd, shell=True, capture_output=True, universal_newlines=True)

        timing = round(time.time() - t0, 3)
        status = ret.returncode

        # hum, not clear that status==0 means actually success , workaround in the meantime.

        for line in (ret.stdout + ret.stderr).split('\n'):
            # workaround for ingest
            if 'Failed to ingest' in line:
                log = logger.fatal
                status = -1
            elif 'FATAL' in line:
                log = logger.fatal
                status = -1
            elif 'WARN' in line:
                log = logger.warning
            else:
                log = logger.info

            if verbose:
                log(line)

        statusStr = 'OK' if status == 0 else 'FAILED'
        return status, statusStr, timing