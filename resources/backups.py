from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table


def get_resources_usage(resources, meta, project_id=None):
    backups = Table('backups', meta, autoload=True)
    if project_id:
        resources_usage = select(
            columns=[
                backups.c.project_id,
                func.count(backups.c.id),
                func.sum(backups.c.size)
            ],
            whereclause=and_(backups.c.deleted == 0,
                             backups.c.project_id == project_id),
            group_by=[backups.c.project_id])
    else:
        resources_usage = select(
            columns=[
                backups.c.project_id,
                func.count(backups.c.id),
                func.sum(backups.c.size)
            ],
            whereclause=and_(backups.c.deleted == 0),
            group_by=[backups.c.project_id])
    for (project_id, backups, gigabytes) in resources_usage.execute():
        if project_id not in resources:
            resources[project_id] = {}
            resources[project_id]['project_id'] = project_id
        resources[project_id]['backups'] = int(backups)
        resources[project_id]['backup_gigabytes'] = int(gigabytes)
    return resources
