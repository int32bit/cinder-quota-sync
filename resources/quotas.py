#!/usr/bin/env python
#
# Copyright (c) 2014 CERN
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Author:
#  Belmiro Moreira <belmiro.moreira@cern.ch>

import argparse
import sys
import ConfigParser
import datetime

from prettytable import PrettyTable
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


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
