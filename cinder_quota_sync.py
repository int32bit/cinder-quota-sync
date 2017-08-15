import argparse
import sys

from prettytable import PrettyTable

import db
from resources import backups as backups_resource
from resources import quotas
from resources import snapshots as snapshots_resource
from resources import volume_types
from resources import volumes as volumes_resource


RESOURCES = [
    'volumes', 'gigabytes', 'backups', 'backup_gigabytes', 'snapshots'
]


def update_quota_usages(meta, usage):

    if usage['in_sync']:
        print("[ERROR] already in sync")
        return

    keys = get_all_rows(RESOURCES, volume_types.get_names())
    for key in keys:
        quota_key = key + '#quota_usage'
        if usage[key] != usage[quota_key]:
            db.update_quota_usages_db(meta, usage['project_id'], key,
                                      usage[key])


def get_all_rows(resources, volume_types):
    rows = []
    for r in resources:
        rows.append(r)
        # backup has no type
        if not r.startswith('backup'):
            for t in volume_types:
                rows.append(r + '_' + t)
    return rows


def get_rows(resources, volume_type=None):
    rows = []
    suffix = '_%s' % volume_type if volume_type else ''
    for resource in resources:
        if resource.startswith('backup') and volume_type:
            # backup has no type
            continue
        else:
            rows.append(resource + suffix)
    return rows


def display(resources, volume_type=None, all_resources=False):

    keys = get_rows(RESOURCES, volume_type)
    rows = ['project_id'] + keys + ['status']
    ptable = PrettyTable(rows)
    for project_id in resources:
        values = [project_id]
        in_sync = True
        for key in keys:
            quota_key = key + '#quota_usage'
            if resources[project_id][key] != resources[project_id][quota_key]:
                in_sync = False
                value = str(resources[project_id][quota_key]) + ' -> ' + str(
                    resources[project_id][key])
            else:
                value = str(resources[project_id][key])
            values.append(value)
        if not in_sync:
            values.append('\033[1m\033[91mMismatch\033[0m')
            ptable.add_row(values)
        elif all_resources:
            values.append('\033[1m\033[92mOK\033[0m')
            ptable.add_row(values)
    if ptable._rows:
        print('\n')
        print(ptable)


def analise_user_usage(resources):
    keys = get_all_rows(RESOURCES, volume_types.get_names())
    for project_id in resources:
        in_sync = True
        for key in keys:
            quota_key = key + '#quota_usage'
            if key not in resources[project_id]:
                resources[project_id][key] = 0
            if quota_key not in resources[project_id]:
                resources[project_id][quota_key] = 0
            if resources[project_id][key] != resources[project_id][quota_key]:
                in_sync = False
        resources[project_id]['in_sync'] = in_sync
    return resources


def sync_resources(meta, resources):
    for project_id in resources:
        if not resources[project_id]['in_sync']:
            update_quota_usages(meta, resources[project_id])


def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all",
        action="store_true",
        help="show the state of all quota resources")
    parser.add_argument(
        "--auto_sync",
        action="store_true",
        help=("automatically sync all resources, "
              "PLEASE USE IT WITH EXTREME CAUTION."))
    parser.add_argument(
        "--project_id", type=str, help="searches only project ID")
    parser.add_argument(
        "--config",
        default='/etc/cinder/cinder.conf',
        help='configuration file')
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Wrong command line arguments (%s)" % e.strerror)

    db_url = db.get_db_url(args.config)
    cinder_session, cinder_metadata, cinder_Base = db.makeConnection(db_url)
    resources = {}
    volume_types.refresh(cinder_metadata)
    for t in [None] + volume_types.get_ids():
        # Update volumes.
        volumes_resource.get_resources_usage(
            resources,
            cinder_metadata,
            project_id=args.project_id,
            volume_type_id=t)
        # Update snapshots.
        snapshots_resource.get_resources_usage(
            resources,
            cinder_metadata,
            project_id=args.project_id,
            volume_type_id=t)
    # Update backups.
    backups_resource.get_resources_usage(
        resources, cinder_metadata, project_id=args.project_id)
    # Update quota usage.
    quotas.get_resources_usage(
        resources, cinder_metadata, project_id=args.project_id)
    # Update in_sync and fill in.
    analise_user_usage(resources)
    for t in [None] + volume_types.get_names():
        display(resources, volume_type=t, all_resources=args.all)
    if args.auto_sync:
        sync_resources(cinder_metadata, resources)


if __name__ == "__main__":
    main()
