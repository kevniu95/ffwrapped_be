from typing import List, Dict
from sqlalchemy import create_engine, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker, Session
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


def bulk_upsert_players_with_ids(
    records: List[Dict], db: Session = None
) -> List[orm.Player]:
    if db is None:
        logger.error(
            "No valid db was supplied to method to bulk upsert players with ids!"
        )
        return None
    try:
        stmt = pg_insert(orm.Player).values(records)
        update_dict = {
            c.name: c
            for c in stmt.excluded
            if c.name not in ["player_id", "pfref_id", "first_name", "last_name"]
        }
        logger.info(f"Update the columns: {update_dict}")
        stmt = stmt.on_conflict_do_update(index_elements=["pfref_id"], set_=update_dict)
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.error(f"Error in bulk upserting players with ids: {e}")
        db.rollback()
        raise
    logger.info("Successfully bulk upserted players with ids")
    return records


def bulk_insert(
    records: list[orm.Base], record_type: orm.Base, flush: bool = False, db=None
) -> list[orm.Base]:
    """
    Returns returning orm
    """
    # TODO: default to fastapi_sqlalchemy db later on
    # Then remove the db=None parameter
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        records = db.scalars(insert(record_type).returning(record_type), records)
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


def insert_record(record: orm.Base, flush: bool = False, db=None) -> orm.Base:

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


def get_platform_by_name(platform_name: str, db=None) -> orm.Platform:
    try:
        platform = (
            db.query(orm.Platform)
            .filter(orm.Platform.platform_name == platform_name)
            .first()
        )
    except:
        logger.error(f"Error in retrieving platform named {platform_name} from db")
        db.rollback()
        raise
    return platform


def get_player_metadata_by_season_chunk(
    season: int, chunk: int, db=None
) -> orm.PlayerWeekMetadata:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        metadata = (
            db.query(orm.PlayerWeekMetadata)
            .filter(
                orm.PlayerWeekMetadata.season == season,
                orm.PlayerWeekMetadata.chunk_start_value == chunk,
            )
            .first()
        )
    except:
        logger.error(f"Error in retrieving player metadata chunk {chunk} from db")
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return metadata


def get_players_by_pfref_id(pfref_ids: List[int], db=None) -> List[orm.Player]:
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        players = db.query(orm.Player).filter(orm.Player.pfref_id.in_(pfref_ids)).all()
    except:
        logger.error("Error in getting players by pfref id")
        db.rollback()
        raise
    finally:
        if new_session:
            db.close()
    return players


def get_league_season_by_platform_league_id(
    league_id: str | int, db: Session = None
) -> orm.LeagueSeason:
    if db is None:
        logger.error(
            "No valid db was surprised to method to get league id from league platform id!"
        )
        return None
    try:
        league = (
            db.query(orm.LeagueSeason)
            .filter(orm.LeagueSeason.platform_league_id.in_([str(league_id)]))
            .one_or_none()
        )
    except:
        logger.error("Error in getting league by platform league id")
        db.roll_back()
        raise
    return league


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


def execute_text_command(txt: str, db) -> None:
    try:
        db.execute(text(txt))
    except:
        db.rollback()
        raise
