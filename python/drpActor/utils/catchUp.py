"""Compare what opdb says was taken with what the butler holds, to find the visits left to ingest or reduce."""

from collections import defaultdict

from ics.utils.opdb import opDB
from lsst.daf.butler import MissingDatasetTypeError


def takenExposures(lookback, settle=60):
    """
    SPS exposures that ended within a time window, from opdb.

    Parameters
    ----------
    lookback : float
        Hours before now the exposures may have started.
    settle : float
        Seconds an exposure must have ended for, so that its file is written.

    Returns
    -------
    exposures : dict
        {visit: {(arm, spectrograph)}}.
    """
    # opdb times are HST.
    sql = ('SELECT sps_exposure.pfs_visit_id, sps_camera.arm, sps_camera.sps_module_id '
           'FROM sps_exposure INNER JOIN sps_camera ON sps_exposure.sps_camera_id = sps_camera.sps_camera_id '
           "WHERE sps_exposure.time_exp_start > (now() AT TIME ZONE 'Pacific/Honolulu') - %(lookback)s * interval '1 hour' "
           "AND sps_exposure.time_exp_end < (now() AT TIME ZONE 'Pacific/Honolulu') - %(settle)s * interval '1 second'")

    exposures = defaultdict(set)

    for visit, arm, spectrograph in opDB.fetchall(sql, dict(lookback=lookback, settle=settle)):
        exposures[int(visit)].add((str(arm), int(spectrograph)))

    return dict(exposures)


def visitExposures(visits):
    """
    SPS exposures of the given visits, from opdb.

    Parameters
    ----------
    visits : iterable of int
        Visits.

    Returns
    -------
    exposures : dict
        {visit: {(arm, spectrograph)}}, visits without exposure being absent.
    """
    visits = sorted(set(visits))

    if not visits:
        return dict()

    sql = ('SELECT sps_exposure.pfs_visit_id, sps_camera.arm, sps_camera.sps_module_id '
           'FROM sps_exposure INNER JOIN sps_camera ON sps_exposure.sps_camera_id = sps_camera.sps_camera_id '
           'WHERE sps_exposure.pfs_visit_id IN %(visits)s')

    exposures = defaultdict(set)

    for visit, arm, spectrograph in opDB.fetchall(sql, dict(visits=tuple(visits))):
        exposures[int(visit)].add((str(arm), int(spectrograph)))

    return dict(exposures)


def ingestedExposures(butler, collection, visits):
    """
    Raw exposures of the given visits held by the butler.

    Parameters
    ----------
    butler : lsst.daf.butler.Butler
        Butler.
    collection : str
        Collection of the raw datasets.
    visits : iterable of int
        Visits.

    Returns
    -------
    exposures : dict
        {visit: {(arm, spectrograph)}}, visits without raw dataset being absent.
    """
    visits = sorted(set(visits))

    if not visits:
        return dict()

    refs = butler.registry.queryDatasets('raw', collections=collection, instrument='PFS',
                                         where=f'visit IN ({",".join(map(str, visits))})')
    exposures = defaultdict(set)

    for ref in refs:
        exposures[ref.dataId['visit']].add((ref.dataId['arm'], ref.dataId['spectrograph']))

    return dict(exposures)


def missingExposures(taken, ingested):
    """
    Exposures taken but not ingested.

    Parameters
    ----------
    taken : dict
        {visit: {(arm, spectrograph)}} of the exposures taken.
    ingested : dict
        {visit: {(arm, spectrograph)}} of the exposures ingested.

    Returns
    -------
    missing : dict
        {visit: {(arm, spectrograph)}}, visits with nothing missing being absent.
    """
    missing = {visit: cams - ingested.get(visit, set()) for visit, cams in taken.items()}
    return {visit: cams for visit, cams in missing.items() if cams}


def ingestedPfsConfigs(butler, collection, visits):
    """
    Visits whose pfsConfig the butler holds.

    Parameters
    ----------
    butler : lsst.daf.butler.Butler
        Butler.
    collection : str
        Collection of the pfsConfig datasets.
    visits : iterable of int
        Visits.

    Returns
    -------
    visits : set of int
        Visits holding a pfsConfig.
    """
    visits = sorted(set(visits))

    if not visits:
        return set()

    try:
        refs = butler.registry.queryDatasets('pfsConfig', collections=collection, instrument='PFS',
                                             where=f'visit IN ({",".join(map(str, visits))})')
        return {ref.dataId['visit'] for ref in refs}
    # a repo where no pfsConfig was ever ingested.
    except MissingDatasetTypeError:
        return set()


def reducedVisits(butler, collection, visits, datasetType='postISRCCD'):
    """
    Visits holding a dataset of the given type, marking them reduced.

    The default, isr's output, marks a visit reduced once its reduction got past isr, whatever happened next.

    Parameters
    ----------
    butler : lsst.daf.butler.Butler
        Butler.
    collection : str
        Collection searched, the reductions chain.
    visits : iterable of int
        Visits.
    datasetType : str
        Dataset type marking a reduced visit.

    Returns
    -------
    reduced : set of int
        Visits holding at least one dataset of that type.
    """
    visits = sorted(set(visits))

    if not visits:
        return set()

    try:
        refs = butler.registry.queryDatasets(datasetType, collections=collection, instrument='PFS',
                                             where=f'visit IN ({",".join(map(str, visits))})')
        return {ref.dataId['visit'] for ref in refs}
    # a repo where nothing was ever reduced.
    except MissingDatasetTypeError:
        return set()


def sequenceVisits(sequenceId):
    """
    Visits of an IIC sequence and their exposure type, from opdb.

    Parameters
    ----------
    sequenceId : int
        iic_sequence.iic_sequence_id.

    Returns
    -------
    expTypes : dict
        {visit: sps_visit.exp_type}, empty if the sequence has no SPS visit.
    """
    sql = ('SELECT sps_visit.pfs_visit_id, sps_visit.exp_type '
           'FROM visit_set INNER JOIN sps_visit ON visit_set.pfs_visit_id = sps_visit.pfs_visit_id '
           'WHERE visit_set.iic_sequence_id = %(sequenceId)s')

    return {int(visit): str(expType) for visit, expType in opDB.fetchall(sql, dict(sequenceId=int(sequenceId)))}


def finishedSequences(visits):
    """
    Finished IIC sequences holding any of the given visits, from opdb.

    Parameters
    ----------
    visits : iterable of int
        Visits.

    Returns
    -------
    sequenceTypes : dict
        {sequenceId: iic_sequence.sequence_type}.
    """
    visits = sorted(set(visits))

    if not visits:
        return dict()

    sql = ('SELECT DISTINCT iic_sequence.iic_sequence_id, iic_sequence.sequence_type '
           'FROM visit_set INNER JOIN iic_sequence ON visit_set.iic_sequence_id = iic_sequence.iic_sequence_id '
           'INNER JOIN iic_sequence_status ON iic_sequence.iic_sequence_id = iic_sequence_status.iic_sequence_id '
           'WHERE visit_set.pfs_visit_id IN %(visits)s AND iic_sequence_status.finished_at IS NOT NULL')

    rows = opDB.fetchall(sql, dict(visits=tuple(visits)))
    return {int(sequenceId): str(sequenceType) for sequenceId, sequenceType in rows}


def sequenceType(sequenceId):
    """
    Type of an IIC sequence, from opdb.

    Parameters
    ----------
    sequenceId : int
        iic_sequence.iic_sequence_id.

    Returns
    -------
    sequenceType : str or None
        iic_sequence.sequence_type, None if the sequence does not exist.
    """
    rows = opDB.fetchall('SELECT sequence_type FROM iic_sequence WHERE iic_sequence_id = %(sequenceId)s',
                         dict(sequenceId=int(sequenceId)))
    return str(rows[0][0]) if len(rows) else None
