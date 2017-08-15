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
import os

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

from resources import volumes as volumes_resource
from resources import snapshots as snapshots_resource
from resources import backups as backups_resource
from resources import volume_types
from resources import quotas


def makeConnection(db_url):
    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    thisSession = Session()
    metadata = MetaData()
    metadata.bind = engine
    Base = declarative_base()
    tpl = thisSession, metadata, Base

    return tpl


def update_quota_usages_db(meta, project_id, resource, in_use):
    quota_usages = Table('quota_usages', meta, autoload=True)
    now = datetime.datetime.utcnow()
    quota = select(
        columns=[quota_usages.c.project_id],
        whereclause=and_(
            quota_usages.c.deleted == 0,
            quota_usages.c.project_id == project_id,
            quota_usages.c.resource == resource)).execute().fetchone()

    if not quota:
        quota_usages.insert().values(
            created_at=now,
            updated_at=now,
            project_id=project_id,
            resource=resource,
            reserved=0,
            deleted=0,
            user_id=user_id).execute()
    else:
        quota_usages.update().where(
            and_(quota_usages.c.project_id == project_id,
                 quota_usages.c.resource == resource)).values(
                     updated_at=now, in_use=in_use).execute()


def get_db_url(config_file):
    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection')
    except Exception:
        print "ERROR: Check nova configuration file."
        sys.exit(2)
    return db_url
