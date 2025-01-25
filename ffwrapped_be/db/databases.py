from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from ffwrapped_be.config import config
from ffwrapped_be.app.data_models import orm

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