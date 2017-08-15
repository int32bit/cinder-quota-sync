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

VOLUME_TYPE_MAP = None


class VolumeTypeNotFound(Exception):
    pass


class VolumeTypeNotInitialize(Exception):
    pass


def refresh(meta):
    global VOLUME_TYPE_MAP
    if VOLUME_TYPE_MAP is None:
        VOLUME_TYPE_MAP = {}
    types = Table('volume_types', meta, autoload=True)
    resources = select(
        columns=[types.c.id, types.c.name], whereclause=types.c.deleted == 0)
    for (id, name) in resources.execute():
        VOLUME_TYPE_MAP[id] = name


def get_volume_type_by_id(volume_type_id):
    global VOLUME_TYPE_MAP
    if VOLUME_TYPE_MAP is None:
        raise VolumeTypeNotInitialize()
    try:
        return VOLUME_TYPE_MAP[volume_type_id]
    except KeyError:
        raise VolumeTypeNotFound(
            "volume type '%s' not found." % volume_type_id)


def get_ids():
    global VOLUME_TYPE_MAP
    if VOLUME_TYPE_MAP is None:
        raise VolumeTypeNotInitialize()
    return VOLUME_TYPE_MAP.keys()


def get_names():
    global VOLUME_TYPE_MAP
    if VOLUME_TYPE_MAP is None:
        raise VolumeTypeNotInitialize()
    return VOLUME_TYPE_MAP.values()
