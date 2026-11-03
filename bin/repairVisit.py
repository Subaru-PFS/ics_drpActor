#!/usr/bin/env python
"""Ingest the raw files of a visit missing from the butler, updating its existing visit and exposure records.

A visit ingested in two parts gets its records from the first part; ingesting the rest then conflicts with them
(e.g. the NIR exposure time against the CCD one). This ingests what is missing with update_exposure_records, so the
records take the values of the files ingested here.

usage: repairVisit.py --datastore /data/drp/datastore --run PFS/raw/sps --transfer link 153149 [--dry-run]
"""
import argparse

from lsst.daf.butler import Butler
from lsst.obs.base.ingest import RawIngestConfig
from lsst.obs.pfs.gen3 import PfsRawIngestTask

from drpActor.utils.files import visitFiles


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('visit', type=int)
    parser.add_argument('--datastore', required=True)
    parser.add_argument('--run', default='PFS/raw/sps')
    parser.add_argument('--transfer', default='link', help='copy|link, as the actor ingests at that site')
    parser.add_argument('--dry-run', action='store_true', help='only list the files missing from the butler')
    args = parser.parse_args()

    butler = Butler(args.datastore, run=args.run)
    _, exposureFiles = visitFiles(args.visit)

    for file in exposureFiles:
        file.initialize(butler)

    missing = [file.filepath for file in exposureFiles if not file.ingested]
    print(f'visit {args.visit}: {len(exposureFiles)} files on disk, {len(missing)} missing from {args.run}')
    for filepath in missing:
        print(f'  {filepath}')

    if not missing or args.dry_run:
        return

    config = RawIngestConfig()
    config.transfer = args.transfer
    task = PfsRawIngestTask(config=config, butler=butler)
    task.run(missing, update_exposure_records=True)

    for file in exposureFiles:
        file.initialize(butler)

    print(f'visit {args.visit}: {sum(not file.ingested for file in exposureFiles)} files still missing')


if __name__ == '__main__':
    main()
