from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    last_name = Column(String(50))

    seasons = relationship("PlayerSeason", back_populates="player")
    weekly_teams = relationship("PlayerWeeklyTeam", back_populates="player")
    weeks = relationship("PlayerWeek", back_populates="player")
	
class PlayerSeason(Base):
    __tablename__ = 'player_season'
    player_season_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    season = Column(Integer, nullable=False)
    position = Column(String(50), nullable=False)
    player = relationship("Player", back_populates="seasons")

    __table_args__ = (UniqueConstraint('player_id', 'season'),)
				
class PlayerWeeklyTeam(Base):
    __tablename__ = 'player_team'
    player_team_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    season = Column(Integer, nullable= False)
    week = Column(Integer, nullable = False)
    tm_id = Column(Integer)
    player = relationship("Player", back_populates="weekly_teams")

    __table_args__ = (UniqueConstraint('player_id', 'season', 'week'),)

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)

class TeamName(Base):
    __tablename__ = 'team_names'
    team_name_id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable = False)
    tm_id = Column(Integer, ForeignKey('teams.team_id'))
    team_name = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint('season', 'tm_id'),)  
  
class PlayerWeek(Base):
    __tablename__ = 'player_week'
    player_week_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    season = Column(Integer, nullable= False)
    week = Column(Integer, nullable = False)
    points = Column(Integer)
    player = relationship("Player", back_populates="weeks")


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    tm_id = Column(Integer, ForeignKey('teams.team_id'))
    opp_id = Column(Integer, ForeignKey('teams.team_id'))
    home_away = Column(String(50))

    __table_args__ = (UniqueConstraint('season', 'week', 'tm_id'),)