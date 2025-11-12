"""Database operations for storing conference data."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from .models import Calling, Conference, get_speaker


def setup_sql() -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Initialize SQLite database with required tables."""
    logger = logging.getLogger(__name__)
    db_path = Path("conference_talks.db")
    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE speakers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE organizations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE callings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            organization INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            UNIQUE(name, organization) ON CONFLICT IGNORE,
            FOREIGN KEY(organization) REFERENCES organizations
        )
    """)
    cur.execute("""
        CREATE TABLE conferences(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year NOT NULL,
            season TEXT NOT NULL,
            UNIQUE(year, season) ON CONFLICT IGNORE
        )
    """)
    cur.execute("""
        CREATE TABLE talks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            emeritus INTEGER NOT NULL DEFAULT 0,
            conference INTEGER NOT NULL,
            UNIQUE(title, conference) ON CONFLICT IGNORE,
            FOREIGN KEY(conference) REFERENCES conferences
        )
    """)
    cur.execute("""
        CREATE TABLE talk_speakers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            speaker INTEGER NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
            FOREIGN KEY(speaker) REFERENCES speakers
        )
    """)
    cur.execute("""
        CREATE TABLE talk_callings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            calling INTEGER NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
            FOREIGN KEY(calling) REFERENCES callings
        )
    """)
    cur.execute("""
        CREATE TABLE talk_texts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER UNIQUE NOT NULL,
            text TEXT NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)
    cur.execute("""
        CREATE TABLE talk_urls(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            url TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind in ('audio', 'video', 'text')),
            UNIQUE(talk, url) ON CONFLICT IGNORE
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)
    cur.execute("""
        CREATE TABLE talk_topics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE(talk, name) ON CONFLICT IGNORE
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)

    logger.info("Configured SQLite database")
    return con, cur


def save_sql(con: sqlite3.Connection, cur: sqlite3.Cursor, conference_df: pd.DataFrame) -> None:
    """Save conference data to SQLite database."""
    logger = logging.getLogger(__name__)
    speakers: list[str] = []
    orgs: list[str] = []
    conferences: set[Conference] = set()
    talks: list[tuple[str, int]] = []
    callings: list[Calling] = []

    for idx, row in conference_df.iterrows():
        speaker = get_speaker(row.speaker)
        if not speaker:
            logger.warning(f"Talk has no speaker: {row.title} ({row.year} {row.season})")
        speakers.append(speaker)

        calling = Calling(row.calling)
        if not calling:
            logger.warning(f"Talk has no calling: {row.title} ({row.year} {row.season})")
        orgs.append(calling.organization)
        callings.append(calling)

        conferences.add(Conference(row.year, row.season))
        talks.append((row.title, 1 if calling.emeritus else 0))

    cur.executemany(
        "INSERT INTO conferences (year, season) VALUES (:year, :season)",
        map(lambda c: c.__dict__, conferences),
    )
    cur.executemany(
        "INSERT INTO speakers (name) VALUES (?)",
        map(lambda v: (v,), filter(lambda v: v, set(speakers))),
    )
    cur.executemany(
        "INSERT INTO organizations (name) VALUES (?)",
        map(lambda v: (v,), filter(lambda v: v, set(orgs))),
    )
    con.commit()

    # Now that the easy things are inserted, query for foreign key IDs
    for idx, row in conference_df.iterrows():
        talk, emeritus = talks[idx]

        conference_id = cur.execute(
            "SELECT id FROM conferences WHERE year = ? AND season = ?",
            (row.year, row.season),
        ).fetchone()[0]
        cur.execute(
            "INSERT INTO talks (title, emeritus, conference) VALUES (?, ?, ?)",
            (talk, emeritus, conference_id),
        )
        talk_id = cur.lastrowid
        speaker = speakers[idx]

        if speaker:
            speaker_id = cur.execute("SELECT id FROM speakers WHERE name = ?", (speaker,)).fetchone()[0]
            cur.execute(
                "INSERT INTO talk_speakers (talk, speaker) VALUES (?, ?)",
                (talk_id, speaker_id),
            )

        calling = callings[idx]
        if calling:
            org_id = cur.execute("SELECT id FROM organizations WHERE name = ?", (calling.organization,)).fetchone()[0]
            cur.execute(
                "INSERT INTO callings (name, organization, rank) VALUES (?, ?, ?)",
                (calling.name, org_id, calling.rank),
            )
            calling_id = cur.lastrowid

            cur.execute(
                "INSERT INTO talk_callings (talk, calling) VALUES (?, ?)",
                (talk_id, calling_id),
            )

        try:
            cur.execute("INSERT INTO talk_texts (talk, text) VALUES (?, ?)", (talk_id, row.talk))
        except sqlite3.IntegrityError:
            logger.exception(f"Failed inserting talk {talk_id} - {talk} - {row.season} {row.year}")
        cur.execute("INSERT INTO talk_urls (talk, url, kind) VALUES (?, ?, 'text')", (talk_id, row.url))
