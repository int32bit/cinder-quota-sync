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

import volume_types as vtype


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
        suffix = "_%s" % vtype.get_volume_type_by_id(
            volume_type_id) if volume_type_id else ''
        resources[project_id]['volumes' + suffix] = int(volumes)
        resources[project_id]['gigabytes' + suffix] = int(gigabytes)
    return resources
