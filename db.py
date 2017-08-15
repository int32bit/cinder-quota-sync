import ConfigParser
import datetime
import sys

from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from sqlalchemy import Table


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
        print("ERROR: Check cinder configuration file.")
        sys.exit(2)
    return db_url
