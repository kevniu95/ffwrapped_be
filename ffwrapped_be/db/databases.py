from typing import List
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
import logging

from ffwrapped_be.config import config
from ffwrapped_be.app.data_models import orm

logger = logging.getLogger(__name__)

# Create engine
engine = create_engine(config.railway_db_url)

# Create sessionmaker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a new session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def commit(db):
    try:
        db.commit()
    except:
        db.rollback()
        raise
    
def bulk_insert(records: list[orm.Base],
                record_type: orm.Base,
                flush: bool = False,
                db=None) -> list[orm.Base]:
    '''
    Returns returning orm
    '''
    # TODO: default to fastapi_sqlalchemy db later on
    # Then remove the db=None parameter
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        records = db.scalars(
            insert(record_type).returning(record_type), records
        )
        if flush:
            db.flush()
            # TODO: this logic lowkey makes no sense- need to fix
            return records
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return records

def insert_record(record: orm.Base, 
                  flush: bool = False,
                  db=None) -> orm.Base:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        db.add(record)
        if flush:
            db.flush()
            # TODO: this logic lowkey makes no sense- need to fix
            return record
        db.commit()
        db.refresh(record)
    except:
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return record

def get_all_records(record_type: orm.Base, db=None) -> list[orm.Base]:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        records = db.query(record_type).all()
    except:
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return records

def get_player_metadata_by_season_chunk(season: int, chunk: int, db=None) -> orm.PlayerWeekMetadata:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        metadata = (db.query(orm.PlayerWeekMetadata).
                    filter(orm.PlayerWeekMetadata.season == season, 
                           orm.PlayerWeekMetadata.chunk_start_value == chunk
                    ).first())
    except:
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return metadata

def get_players_by_id(pfref_ids: List[int], 
                      db=None) -> List[orm.Player]:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        players = db.query(orm.Player).filter(orm.Player.pfref_id.in_(pfref_ids)).all()
    except:
        logger.error('Error in getting players by ID')
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return players

def delete_all_rows(table: orm.Base, db=None):
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        db.query(table).delete()
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()