from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table

import volume_types


def get_resources_usage(resources, meta, project_id=None, volume_type_id=None):
    volumes = Table('volumes', meta, autoload=True)
    if volume_type_id:
        if project_id:
            resources_usage = select(
                columns=[
                    volumes.c.project_id,
                    func.count(volumes.c.id),
                    func.sum(volumes.c.size)
                ],
                whereclause=and_(volumes.c.deleted == 0,
                                 volumes.c.volume_type_id == volume_type_id,
                                 volumes.c.project_id == project_id),
                group_by=[volumes.c.project_id])
        else:
            resources_usage = select(
                columns=[
                    volumes.c.project_id,
                    func.count(volumes.c.id),
                    func.sum(volumes.c.size)
                ],
                whereclause=and_(volumes.c.deleted == 0,
                                 volumes.c.volume_type_id == volume_type_id),
                group_by=[volumes.c.project_id])
    else:
        if project_id:
            resources_usage = select(
                columns=[
                    volumes.c.project_id,
                    func.count(volumes.c.id),
                    func.sum(volumes.c.size)
                ],
                whereclause=and_(volumes.c.deleted == 0,
                                 volumes.c.project_id == project_id),
                group_by=[volumes.c.project_id])
        else:
            resources_usage = select(
                columns=[
                    volumes.c.project_id,
                    func.count(volumes.c.id),
                    func.sum(volumes.c.size)
                ],
                whereclause=and_(volumes.c.deleted == 0),
                group_by=[volumes.c.project_id])
    for (project_id, volumes, gigabytes) in resources_usage.execute():
        if project_id not in resources:
            resources[project_id] = {}
            resources[project_id]['project_id'] = project_id
        suffix = "_%s" % volume_types.get_volume_type_by_id(
            volume_type_id) if volume_type_id else ''
        resources[project_id]['volumes' + suffix] = int(volumes)
        resources[project_id]['gigabytes' + suffix] = int(gigabytes)
    return resources
