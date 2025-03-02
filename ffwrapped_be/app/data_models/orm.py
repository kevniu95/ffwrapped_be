from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Date,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
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
    seasons = relationship("PlayerSeason", back_populates="player", lazy="joined")


class PlayerSeason(Base):
    __tablename__ = "player_season"
    player_season_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"))
    season = Column(Integer, nullable=False)
    position = Column(String(50), nullable=False)
    player = relationship("Player", back_populates="seasons", lazy="joined")
    weeks = relationship(
        "PlayerWeekESPN", back_populates="player_season", lazy="joined"
    )

    @property
    def espn_weeks_dict(self):
        return {week.week: week for week in self.weeks}

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


class PlayerWeekESPN(Base):
    __tablename__ = "player_week_espn"
    player_week_id = Column(Integer, primary_key=True)
    player_season_id = Column(Integer, ForeignKey("player_season.player_season_id"))
    week = Column(
        Integer,
        nullable=False,
    )
    # Passing Stats
    passing_attempts = Column(Integer)
    passing_completions = Column(Integer)
    passing_yards = Column(Integer)
    passing_touchdowns = Column(Integer)
    passing_40yard_tds = Column(Integer)
    passing_50yard_tds = Column(Integer)
    passing_2pt_conversions = Column(Integer)
    passing_interceptions = Column(Integer)
    # Rushing Stats
    rushing_attempts = Column(Integer)
    rushing_yards = Column(Integer)
    rushing_touchdowns = Column(Integer)
    rushing_40yard_tds = Column(Integer)
    rushing_50yard_tds = Column(Integer)
    rushing_2pt_conversions = Column(Integer)
    # Receiving Stats
    receiving_targets = Column(Integer)
    receiving_receptions = Column(Integer)
    receiving_yards = Column(Integer)
    receiving_touchdowns = Column(Integer)
    receiving_40yard_tds = Column(Integer)
    receiving_50yard_tds = Column(Integer)
    receiving_2pt_conversions = Column(Integer)
    # General offensive stats
    fumbles = Column(Integer)
    fumbles_lost = Column(Integer)
    fumbles_recovered_for_td = Column(Integer)
    passing_sacks = Column(Integer)
    # Kicking stats
    kicking_xpm = Column(Integer)
    kicking_xpa = Column(Integer)
    kicking_fgm_0_39 = Column(Integer)
    kicking_fga_0_39 = Column(Integer)
    kicking_fgm_40_49 = Column(Integer)
    kicking_fga_40_49 = Column(Integer)
    kicking_fgm_50_59 = Column(Integer)
    kicking_fga_50_59 = Column(Integer)
    kicking_fgm_60_plus = Column(Integer)
    kicking_fga_60_plus = Column(Integer)
    # Defensive stats
    defensive_blocked_kick_return_tds = Column(Integer)
    defensive_interceptions = Column(Integer)
    defensive_fumble_recoveries = Column(Integer)
    defensive_blocked_kicks = Column(Integer)
    defensive_safeties = Column(Integer)
    defensive_sacks = Column(Integer)
    kickoff_return_touchdowns = Column(Integer)
    punt_return_touchdowns = Column(Integer)
    interception_return_touchdowns = Column(Integer)
    fumble_return_touchdowns = Column(Integer)
    defensive_forced_fumbles = Column(Integer)
    defensive_assisted_tackles = Column(Integer)
    defensive_solo_tackles = Column(Integer)
    defensive_passes_defended = Column(Integer)
    kickoff_return_yards = Column(Integer)
    punt_return_yards = Column(Integer)
    punts_returned = Column(Integer)
    defensive_points_allowed = Column(Integer)
    defensive_yards_allowed = Column(Integer)
    defensive_2pt_return = Column(Integer)

    __table_args__ = (UniqueConstraint("player_season_id", "week"),)
    player_season = relationship("PlayerSeason", lazy="joined")
    league_weekly_team = relationship(
        "LeagueWeeklyTeam", back_populates="player_week", lazy="joined"
    )


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
    lineup_config = Column(JSONB)
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
    league_weekly_team = relationship("LeagueWeeklyTeam", back_populates="league_team")

    __table_args__ = (UniqueConstraint("league_season_id", "platform_team_id"),)


class DraftTeam(Base):
    __tablename__ = "draft_team"
    league_team_id = Column(
        Integer, ForeignKey("league_team.league_team_id"), primary_key=True
    )
    player_id = Column(Integer, ForeignKey("player.player_id"), primary_key=True)
    draft_pick_number = Column(Integer, nullable=False)


class LeagueWeeklyTeam(Base):
    __tablename__ = "league_weekly_team"
    league_team_id = Column(
        Integer, ForeignKey("league_team.league_team_id"), primary_key=True
    )
    player_week_id = Column(
        Integer, ForeignKey("player_week_espn.player_week_id"), primary_key=True
    )
    lineup_position = Column(String(50), nullable=False)
    league_team = relationship(
        "LeagueTeam", back_populates="league_weekly_team", lazy="joined"
    )

    player_week = relationship(
        "PlayerWeekESPN", back_populates="league_weekly_team", lazy="joined"
    )
