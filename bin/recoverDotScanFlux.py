#!/usr/bin/env python3
"""Reduce the dot-scan visits whose flux never reached dot_roach_flux.

Drives drpActor.utils.dotRoach.DotRoach directly, outside the actor, over a visit
pair that was taken but not reduced. The init visit must come first: it is the
full-frame flat from which the fiberTraces and the reference spectra are built,
and the scan visit is ratioed against them.

Dry run by default: the rows are written as CSV and nothing reaches opdb. Pass
--insert to write them, which is refused for a visit that already has rows.

Runs on the summit drp host, in the environment that provides lsst.daf.butler and
drpActor. The datastore and opdb host are the site S values.
"""

import argparse
import logging
import os

import pandas as pd
from lsst.daf.butler import Butler

import drpActor.utils.dotRoach as dotRoach
from drpActor.utils.files import CCDFile
from pfs.utils.database.opdb import OpDB

# The unreduced pair: the dotRoachInit flat and the single point of the aborted scan.
INIT_VISIT = 148465
SCAN_VISIT = 148467
NIGHT = '2026-09-01'
CAMS = ['r1', 'r2', 'r3', 'r4']

# From pfs_instdata config/actors/drp.yaml, site S.
DATASTORE = '/data/drp/datastore'
INPUT_COLLECTION = 'PFS/defaults'
OUTPUT_COLLECTION = 'drpActor/reductions'
OPDB_HOST = 'db-ics'
RAW_ROOT = '/data/raw'

ARM_NUM = dict(b=1, r=2, n=3, m=4)


class CapturingOpdb:
    """opdb stand-in that keeps the frames DotRoach would have inserted.

    query_scalar is delegated, so the duplicate check still reads the real table;
    insert is captured unless doInsert is set.
    """

    def __init__(self, opdb, doInsert=False):
        self.opdb = opdb
        self.doInsert = doInsert
        self.captured = dict()

    def query_scalar(self, sql, params=None):
        return self.opdb.query_scalar(sql, params=params)

    def insert(self, table, df):
        self.captured[table] = pd.concat([self.captured.get(table, pd.DataFrame()), df])

        if self.doInsert:
            self.opdb.insert(table, df)


class StandaloneEngine:
    """The engine members DotRoach uses: a butler to read from, an opdb to write to,
    and the datastore path its calibration scratch directory is derived from."""

    def __init__(self, butler, opdb, datastore):
        self.butler = butler
        self.opdb = opdb
        self.datastore = datastore


def ccdFiles(visit, cams, night=NIGHT, root=RAW_ROOT):
    """CCDFile per camera for one visit, named the way the raw files are."""
    files = []
    for cam in cams:
        arm, specNum = cam[0], int(cam[1])
        filename = f'PFSA{visit:06d}{specNum}{ARM_NUM[arm]}.fits'
        files.append(CCDFile(root, night, filename))

    return files


def alreadyReduced(opdb, visit):
    """Row count for that visit in dot_roach_flux."""
    return opdb.query_scalar('SELECT COUNT(*) FROM dot_roach_flux WHERE pfs_visit_id = :visit_id',
                             params=dict(visit_id=visit))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--insert', action='store_true',
                        help='write the rows to dot_roach_flux instead of only to CSV')
    parser.add_argument('--outputDir', default='.', help='where the CSV files are written')
    parser.add_argument('--initVisit', type=int, default=INIT_VISIT)
    parser.add_argument('--scanVisit', type=int, default=SCAN_VISIT)
    parser.add_argument('--cams', default=','.join(CAMS))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    cams = args.cams.split(',')

    OpDB.set_default_connection(host=OPDB_HOST)
    opdb = OpDB()

    for visit in (args.initVisit, args.scanVisit):
        nRows = alreadyReduced(opdb, visit)
        logging.info(f'visit={visit} has {nRows} rows in dot_roach_flux')
        if nRows and args.insert:
            raise SystemExit(f'visit={visit} already has {nRows} rows, refusing to insert')

    butler = Butler(DATASTORE, collections=[INPUT_COLLECTION, OUTPUT_COLLECTION])
    capturing = CapturingOpdb(opdb, doInsert=args.insert)
    roach = dotRoach.DotRoach(StandaloneEngine(butler, capturing, DATASTORE), cams)
    logging.info(f'calibration scratch directory: {roach.scratchDir}')

    # The init visit first: it populates fiberTraces, refSpectra and pfsConfig, which
    # the scan visit is then measured against.
    for visit in (args.initVisit, args.scanVisit):
        capturing.captured.clear()
        roach.runAway(visit, ccdFiles(visit, cams))

        df = capturing.captured['dot_roach_flux']
        path = os.path.join(args.outputDir, f'dot_roach_flux-{visit}.csv')
        df.to_csv(path, index=False)
        logging.info(f'visit={visit} {len(df)} rows -> {path}')
        logging.info(f'  flux_ratio_norm: median={df.flux_ratio_norm.median():.4f} '
                     f'min={df.flux_ratio_norm.min():.4f} max={df.flux_ratio_norm.max():.4f} '
                     f'nan={int(df.flux_ratio_norm.isna().sum())}')

    if not args.insert:
        logging.info('dry run: nothing written to opdb. Re-run with --insert.')


if __name__ == '__main__':
    main()
