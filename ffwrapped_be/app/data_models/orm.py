from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
    TIMESTAMP,
    ForeignKey,
    Date,
    Boolean,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()


class Player(Base):
    __tablename__ = "player"
    player_id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    last_name = Column(String(50))
    pfref_id = Column(String(50), nullable=False, index=True)
    espn_id = Column(String(50), index=True)
    sleeper_bot_id = Column(String(50))
    fantasy_pros_id = Column(String(50))
    yahoo_id = Column(String(50))
    cbs_player_id = Column(String(50))

    __table_args__ = (UniqueConstraint("pfref_id"),)
    seasons = relationship("PlayerSeason", back_populates="player")
    weeks = relationship("PlayerWeek", back_populates="player")


class PlayerSeason(Base):
    # TODO: Ignore for now
    # Will fill this when doing later analysis (i.e., projections)
    __tablename__ = "player_season"
    player_season_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"))
    season = Column(Integer, nullable=False)
    position = Column(String(50), nullable=False)
    player = relationship("Player", back_populates="seasons")

    __table_args__ = (UniqueConstraint("player_id", "season"),)


class Team(Base):
    __tablename__ = "team"
    team_id = Column(Integer, primary_key=True)
    team_pfref_id = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint("team_pfref_id"),)

    def __repr__(self):
        return f"<Team(team_id={self.team_id}, team_pfref_id={self.team_pfref_id})>"


class TeamName(Base):
    __tablename__ = "team_name"
    team_name_id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    tm_id = Column(Integer, ForeignKey("team.team_id"))
    team_name = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint("season", "tm_id"),)


class PlayerWeek(Base):
    __tablename__ = "player_week"
    player_week_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"))
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    tm_id = Column(Integer, ForeignKey("team.team_id"))
    pass_cmp = Column(Integer)
    pass_att = Column(Integer)
    pass_yds = Column(Integer)
    pass_td = Column(Integer)
    pass_int = Column(Integer)
    sacks = Column(Integer)
    sack_yds = Column(Integer)
    rush_att = Column(Integer)
    rush_yds = Column(Integer)
    rush_td = Column(Integer)
    targets = Column(Integer)
    receptions = Column(Integer)
    rec_yds = Column(Integer)
    rec_td = Column(Integer)
    fumbles = Column(Integer)
    xpm = Column(Integer)
    xpa = Column(Integer)
    fgm = Column(Integer)
    fga = Column(Integer)
    points = Column(Float)

    __table_args__ = (UniqueConstraint("player_id", "season", "week"),)
    player = relationship("Player", back_populates="weeks")


class PlayerWeekMetadata(Base):
    __tablename__ = "player_week_metadata"
    player_week_metadata_id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    chunk_start_value = Column(Integer)
    chunk_size = Column(Integer)
    completed = Column(Boolean, default=False)
    updated_at = Column(
        TIMESTAMP(timezone=False), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (UniqueConstraint("season", "chunk_start_value", "chunk_size"),)


class Game(Base):
    __tablename__ = "game"
    game_id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    tm_id = Column(Integer, ForeignKey("team.team_id"))
    opp_id = Column(Integer, ForeignKey("team.team_id"))
    home_away = Column(String(50))
    game_date = Column(Date)

    __table_args__ = (UniqueConstraint("season", "week", "tm_id"),)


class Platform(Base):
    __tablename__ = "platform"
    platform_id = Column(Integer, primary_key=True)
    platform_name = Column(String(50), nullable=False)


class LeagueSeason(Base):
    __tablename__ = "league_season"
    league_season_id = Column(Integer, primary_key=True)
    platform_id = Column(Integer, ForeignKey("platform.platform_id"))
    platform_league_id = Column(String(50), nullable=False)
    season = Column(Integer, nullable=False)
    scoring_config = Column(JSONB)
    league_teams = relationship("LeagueTeam", backref="season")

    __table_args__ = (UniqueConstraint("platform_id", "platform_league_id", "season"),)


class LeagueTeam(Base):
    __tablename__ = "league_team"
    league_team_id = Column(Integer, primary_key=True)
    league_season_id = Column(Integer, ForeignKey("league_season.league_season_id"))
    platform_team_id = Column(String(50), nullable=False)
    team_name = Column(String(50))
    team_abbreviation = Column(String(50))

    __table_args__ = (UniqueConstraint("league_season_id", "platform_team_id"),)


class DraftTeam(Base):
    __tablename__ = "draft_team"
    league_team_id = Column(
        Integer, ForeignKey("league_team.league_team_id"), primary_key=True
    )
    player_id = Column(Integer, ForeignKey("player.player_id"), primary_key=True)
    draft_pick_number = Column(Integer, nullable=False)


class WeeklyStarter(Base):
    __tablename__ = "weekly_starter"
    league_team_id = Column(
        Integer, ForeignKey("league_team.league_team_id"), primary_key=True
    )
    week = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"), primary_key=True)
    start = Column(Boolean, nullable=False)


# Transactions
# league_team_id	transaction_type	player_id	timestamp
# TODO: Add this table later
