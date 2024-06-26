import glob
import os

from ics.utils.sps.spectroIds import SpectroIds


def makeArmNumPattern(arms):
    """
    Create a glob pattern for matching arm numbers.

    Parameters:
    arms (str): A string of arm names separated by '^' characters.

    Returns:
    str: A glob pattern for matching arm numbers.
    """
    if not arms:
        return '*'

    armNums = ''.join([str(SpectroIds.validArms[arm]) for arm in arms.split('^')])
    return f'[{armNums}]'


def makeSpecNumPattern(spectrograph):
    """
    Create a glob pattern for matching spectrograph numbers.

    Parameters:
    spectrograph (str): A string of spectrograph names separated by '^' characters.

    Returns:
    str: A glob pattern for matching spectrograph numbers.
    """
    if not spectrograph:
        return '*'

    specNums = ''.join([str(specNum) for specNum in spectrograph.split('^')])
    return f'[{specNums}]'


def makeVisitList(visits):
    """
    Create a list of visit numbers.

    Parameters:
    visits (str): A string of visit numbers separated by '^' characters or a range of visit numbers separated by '..'.

    Returns:
    list: A list of visit numbers.
    """
    if '..' in visits:
        visitMin, visitMax = map(int, visits.split('..'))
        return list(range(visitMin, visitMax + 1))
    else:
        return list(map(int, visits.split('^')))


def makeVisitPattern(visit, spectrograph=None, arms=None):
    """
    Create a glob pattern to match PFS exposure files for a given visit.

    Parameters:
    visit (int): The visit number to match.
    spectrograph (str or None): A string of spectrograph numbers to match, separated by '^', or None to match all spectrographs.
    arms (str or None): A string of arm names to match, separated by '^', or None to match all arms.

    Returns:
    str: A glob pattern that matches PFS exposure files for the given visit, spectrograph(s), and arm(s).
    """
    [pfsConfigPath] = glob.glob(f'/data/raw/*/pfsConfig/pfsConfig*-{visit:06d}.fits')
    pfsConfigDir, _ = os.path.split(pfsConfigPath)
    rootDir, _ = os.path.split(pfsConfigDir)

    specNumPattern = makeSpecNumPattern(spectrograph)
    armNumPattern = makeArmNumPattern(arms)

    return f'{rootDir}/*/PFS*{visit:06d}{specNumPattern}{armNumPattern}.fits'
