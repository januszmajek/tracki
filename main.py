from sqlalchemy import Table, Column, Integer, String, create_engine, MetaData, ForeignKey, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text
import sqlite3
import time

engine = create_engine('sqlite:///essa5.db', echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()

Base = declarative_base()
meta = MetaData()
con = sqlite3.connect("essa5.db")
cursor = con.cursor()


def timer(function):


    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = function(*args, **kwargs)
        time2 = time.time()
        print('{:s} wykonywała się przez {:.2f} sekund'.format(function.__name__, (time2 - time1)))
        return ret


    return wrap


class Sample(Base):


    __tablename__ = "samples"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    track_id = Column(String)
    listening_date = Column(Integer, ForeignKey("tracks.track_id"))


class Track(Base):


    __tablename__ = "tracks"
    id = Column(Integer, primary_key=True)
    ex_id = Column(String)
    track_id = Column(String)
    artist = Column(String)
    track_title = Column(String)


@timer
def create_samples_table():


    samples = Table(
        'samples', meta,
        Column('user_id', String),
        Column('track_id', String, ForeignKey("tracks.track_id")),
        Column('listening_date', Integer)
    )
    meta.create_all(engine)


@timer
def create_tracks_table():


    tracks = Table(
        'tracks', meta,
        Column('id', String),
        Column('track_id', String),
        Column('artist', String),
        Column('track_title', String)
    )


    meta.create_all(engine)


@timer
def load_tracks():


    with open("unique_tracks.txt", encoding="ANSI") as file:
        for line in file:
            row = [tuple(line.strip().split('<SEP>'))]
            cursor.executemany("INSERT or IGNORE INTO tracks (ex_id, track_id, artist, track_title) "
                               "VALUES (?, ?, ?, ?);", row)
        con.commit()


@timer
def load_triplets():


    with open("triplets_sample_20p.txt", encoding="ANSI") as file:
        for line in file:
            row = [tuple(line.strip().split('<SEP>'))]
            cursor.executemany("INSERT or IGNORE INTO samples (user_id, track_id, listening_date) "
                               "VALUES (?, ?, ?);", row)
        con.commit()





@timer
def selecting_artist():

    result = con.execute("SELECT artist FROM (SELECT COUNT(*) as number, track_id FROM samples GROUP BY track_id) tab1 JOIN tracks tab2 on tab1.track_id = tab2.track_id GROUP BY artist ORDER BY SUM(number) DESC LIMIT 1").fetchall()
   
    print("-" * 100)
    print("Najpopularniejszy artysta to:")
    print(result[0][0])


@timer
def selecting_top_tracks():


    result = con.execute("SELECT artist, track_title FROM tracks tab1 JOIN (SELECT track_id, COUNT(*) FROM samples GROUP BY track_id ORDER BY COUNT(*) DESC LIMIT 5) tab2 ON tab1.track_id = tab2.track_id").fetchall()
    
    print("-" * 15)
    print("5 najpopularniejszych piosenek to:")

    for row in result:
        print(row[1])


def disconnecting():


    if con:
        con.close()


Base.metadata.create_all(engine)
if __name__ == '__main__':
    create_tracks_table()
    create_samples_table()

    load_tracks()
    load_triplets()

    selecting_artist()
    selecting_top_tracks()
