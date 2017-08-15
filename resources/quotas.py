from sqlalchemy import and_
from sqlalchemy import select
from sqlalchemy import Table


def get_resources_usage(resources, meta, project_id=None, volume_type_id=None):
    quota_usages = Table('quota_usages', meta, autoload=True)
    if project_id:
        resource_quota_usage = select(
            columns=[
                quota_usages.c.project_id, quota_usages.c.resource,
                quota_usages.c.in_use
            ],
            whereclause=and_(quota_usages.c.deleted == 0,
                             quota_usages.c.project_id == project_id))
    else:
        resource_quota_usage = select(
            columns=[
                quota_usages.c.project_id, quota_usages.c.resource,
                quota_usages.c.in_use
            ],
            whereclause=quota_usages.c.deleted == 0)
    for (project_id, resource, in_use) in resource_quota_usage.execute():
        if project_id not in resources:
            resources[project_id] = {}
            resources[project_id]['project_id'] = project_id
        resources[project_id][resource + '#quota_usage'] = in_use
    return resources
