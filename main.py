from sqlalchemy import Table, Column, Integer, String, create_engine, MetaData, ForeignKey, func, bindparam
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
import sqlite3
import time

# connect with database
engine = create_engine('sqlite:///essa5.db', echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# manage tables
Base = declarative_base()
Base.metadata.create_all(engine)
meta = MetaData()

con = sqlite3.connect("essa5.db")
cursor = con.cursor()

def timer(function):
    """
    function measures execution time
    """

    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = function(*args, **kwargs)
        time2 = time.time()
        print('{:s} function took {:.2f} s'.format(function.__name__, (time2 - time1)))
        return ret

    return wrap


@timer
def create_samples_table():
    samples = Table(
        'samples', meta,
        # Column('id', Integer, primary_key=True),
        Column('user_id', String, primary_key=True),
        Column('track_id', String, ForeignKey("tracks.track_id")),
        Column('listening_date', Integer, ),
        # relationship("Tracks", back_populates='tracks')
    )
    meta.create_all(engine)


@timer
def create_tracks_table():
    tracks = Table(
        'tracks', meta,
        Column('id', String, primary_key=True),
        Column('track_id', String),
        Column('artist', String),
        Column('track_title', String)
        # relationship("Sample", back_populates='tracks')
    )
    meta.create_all(engine)

@timer
def load_tracks():
    """
    insert tracks to database
    """

    with open("unique_tracks.txt", encoding='latin-1') as file:
        for line in file:
            row = [tuple(line.strip().split('<SEP>'))]
            cursor.executemany("INSERT or IGNORE INTO tracks (id, track_id, artist, track_title) "
                               "VALUES (?, ?, ?, ?);", row)


@timer
def loading_triplets():
    """
    function inserts triplets to db
    """

    with open("triplets_sample_20p.txt", encoding="ANSI") as file:
        for line in file:
            row = [tuple(line.strip().split('<SEP>'))]
            cursor.executemany("INSERT or IGNORE INTO samples (user_id, track_id, listening_date) "
                               "VALUES (?, ?, ?);", row)



@timer
def selecting_artist():
    """
    creates query for top artist
    """

    sql = """SELECT artist
                        FROM (SELECT COUNT(*) as number, track_id FROM samples
                        GROUP BY track_id) t1
                        JOIN tracks t2 on t1.track_id = t2.track_id
                        GROUP BY artist
                        ORDER BY SUM(number) DESC
                        LIMIT 1"""
    cursor.execute(sql)

    result = cursor.fetchall()
    print("-" * 100)
    print("Top artist is:")
    print(result[0][0])


@timer
def selecting_top_tracks():
    """
    creates query for top 5 tracks
    """

    sql = """SELECT artist, track_title
                    FROM tracks t1
                    JOIN (SELECT track_id, COUNT(*)
                    FROM samples
                    GROUP BY track_id
                    ORDER BY COUNT(*) DESC 
                    LIMIT 5) t2
                    ON t1.track_id = t2.track_id"""
    cursor.execute(sql)

    result = cursor.fetchall()
    print("-" * 100)
    print("Top 5 tracks are:")
    for row in result:
        print(row[0])


def disconnecting():
    """
    disconnecting from db
    """

    if con:
        con.close()


if __name__ == '__main__':
    create_tracks_table()
    create_samples_table()
    load_tracks()
    loading_triplets()
    selecting_artist()
    selecting_top_tracks()
