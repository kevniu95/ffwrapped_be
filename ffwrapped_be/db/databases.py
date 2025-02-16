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
    if not records:
        logger.info("No records to insert for bulk insert function")
        return []
    # TODO: default to fastapi_sqlalchemy db later on
    # Then remove the db=None parameter
    new_session = False
    if db is None:
        db = SessionLocal()
        new_session = True
    try:
        records = db.scalars(insert(record_type).returning(record_type), records).all()
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
    logger.info(f"Successfully bulk inserted {len(records)} records")
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


def get_players_with_espn_id(db=None) -> List[orm.Player]:
    if not db:
        logger.error("No valid db was supplied to method to get players with ESPN id!")
        return None
    try:
        players = (
            db.query(orm.Player)
            .filter(orm.Player.espn_id.isnot(None))
            .order_by(orm.Player.player_id)
            .all()
        )
    except:
        logger.error("Error in getting players with ESPN id")
        db.rollback()
        raise
    return players


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


def get_players_by_espn_id(espn_ids: List[int], db: Session = None) -> List[orm.Player]:
    if db is None:
        logger.error(
            "No valid db was supplied to method to bulk upsert players with ids!"
        )
        return None
    try:
        players = db.query(orm.Player).filter(orm.Player.espn_id.in_(espn_ids)).all()
    except:
        logger.error("Error in gett players by ESPN id")
        db.rollback()
        raise
    return players


def get_league_season_by_platform_league_id(
    league_id: str | int, season: int, db: Session = None
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
            .filter(orm.LeagueSeason.season == season)
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


def get_draft_team_weekly_rows(
    platform_name: str,
    platform_league_id: str,
    platform_team_id: str,
    db_session: Session,
    week: int = None,
):
    # Query to get the draft team rows
    draft_team_rows = (
        db_session.query(
            orm.PlayerWeek,
            orm.PlayerSeason.position.label("position"),
            orm.Player.first_name.label("first_name"),
            orm.Player.last_name.label("last_name"),
        )
        .join(orm.DraftTeam, orm.PlayerWeek.player_id == orm.DraftTeam.player_id)
        .join(
            orm.LeagueTeam,
            orm.DraftTeam.league_team_id == orm.LeagueTeam.league_team_id,
        )
        .join(
            orm.LeagueSeason,
            orm.LeagueTeam.league_season_id == orm.LeagueSeason.league_season_id,
        )
        .join(orm.Platform, orm.LeagueSeason.platform_id == orm.Platform.platform_id)
        .join(
            orm.PlayerSeason,
            (
                orm.PlayerWeek.player_id == orm.PlayerSeason.player_id
                and orm.PlayerWeek.season == orm.PlayerSeason.season
            ),
        )
        .join(orm.Player, (orm.PlayerWeek.player_id == orm.Player.player_id))
        .filter(
            orm.Platform.platform_name == platform_name,
            orm.LeagueSeason.platform_league_id == platform_league_id,
            orm.LeagueTeam.platform_team_id == platform_team_id,
        )
    )
    if week:
        draft_team_rows = draft_team_rows.filter(orm.PlayerWeek.week == week)
    return draft_team_rows.all()


def get_draft_team_missing(
    platform_name: str,
    platform_league_id: str,
    platform_team_id: str,
    db_session: Session,
):
    # Query to get the draft team rows that don't have player_week entries
    draft_team_missing = (
        db_session.query(orm.DraftTeam)
        .join(
            orm.LeagueTeam,
            orm.DraftTeam.league_team_id == orm.LeagueTeam.league_team_id,
        )
        .join(
            orm.LeagueSeason,
            orm.LeagueTeam.league_season_id == orm.LeagueSeason.league_season_id,
        )
        .join(orm.Platform, orm.LeagueSeason.platform_id == orm.Platform.platform_id)
        .outerjoin(  # Use outerjoin to include rows that don't match
            orm.PlayerWeek,
            (orm.DraftTeam.player_id == orm.PlayerWeek.player_id)
            & (orm.PlayerWeek.season == orm.LeagueSeason.season),
        )
        .filter(
            orm.Platform.platform_name == platform_name,
            orm.LeagueSeason.platform_league_id == platform_league_id,
            orm.LeagueTeam.platform_team_id == platform_team_id,
            orm.PlayerWeek.player_id.is_(
                None
            ),  # Only include rows where there's no matching player_week
        )
        .all()
    )
    return draft_team_missing


def execute_text_command(txt: str, db) -> None:
    try:
        db.execute(text(txt))
    except:
        db.rollback()
        raise
