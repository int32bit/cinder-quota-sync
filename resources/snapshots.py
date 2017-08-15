from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table

import volume_types


def get_resources_usage(resources, meta, project_id=None, volume_type_id=None):
    snapshots = Table('snapshots', meta, autoload=True)
    if volume_type_id:
        if project_id:
            resources_usage = select(
                columns=[snapshots.c.project_id,
                         func.count(snapshots.c.id)],
                whereclause=and_(snapshots.c.deleted == 0,
                                 snapshots.c.volume_type_id == volume_type_id,
                                 snapshots.c.project_id == project_id),
                group_by=[snapshots.c.project_id])
        else:
            resources_usage = select(
                columns=[snapshots.c.project_id,
                         func.count(snapshots.c.id)],
                whereclause=and_(snapshots.c.deleted == 0,
                                 snapshots.c.volume_type_id == volume_type_id),
                group_by=[snapshots.c.project_id])
    else:
        if project_id:
            resources_usage = select(
                columns=[snapshots.c.project_id,
                         func.count(snapshots.c.id)],
                whereclause=and_(snapshots.c.deleted == 0,
                                 snapshots.c.project_id == project_id),
                group_by=[snapshots.c.project_id])
        else:
            resources_usage = select(
                columns=[snapshots.c.project_id,
                         func.count(snapshots.c.id)],
                whereclause=and_(snapshots.c.deleted == 0),
                group_by=[snapshots.c.project_id])
    for (project_id, snapshots) in resources_usage.execute():
        if project_id not in resources:
            resources[project_id] = {}
            resources[project_id]['project_id'] = project_id
        suffix = "_%s" % volume_types.get_volume_type_by_id(
            volume_type_id) if volume_type_id else ''
        resources[project_id]['snapshots' + suffix] = int(snapshots)
    return resources
