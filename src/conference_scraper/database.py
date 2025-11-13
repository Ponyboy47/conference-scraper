"""Database operations for storing conference data."""

import functools
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from .models import Calling, get_speaker

logger = logging.getLogger(__name__)


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


@functools.cache
def get_or_create_speaker(cur: sqlite3.Cursor, name: str) -> int:
    """Get or create a speaker and return their ID. Cached to avoid duplicate operations."""
    cur.execute("INSERT INTO speakers (name) VALUES (?)", (name,))
    return cur.lastrowid


@functools.cache
def get_or_create_organization(cur: sqlite3.Cursor, name: str) -> int:
    """Get or create an organization and return their ID. Cached to avoid duplicate operations."""
    cur.execute("INSERT INTO organizations (name) VALUES (?)", (name,))
    return cur.lastrowid


@functools.cache
def get_or_create_calling(cur: sqlite3.Cursor, name: str, organization_id: int, rank: int) -> int:
    """Get or create a calling and return their ID. Cached to avoid duplicate operations."""
    cur.execute("INSERT INTO callings (name, organization, rank) VALUES (?, ?, ?)", (name, organization_id, rank))
    return cur.lastrowid


@functools.cache
def get_or_create_conference(cur: sqlite3.Cursor, year: int, season: str) -> int:
    """Get or create a conference and return their ID. Cached to avoid duplicate operations."""
    cur.execute("INSERT INTO conferences (year, season) VALUES (?, ?)", (year, season))
    return cur.lastrowid


@functools.cache
def get_or_create_talk(cur: sqlite3.Cursor, title: str, conference_id: int, emeritus: int) -> int:
    """Get or create a talk and return their ID. Cached to avoid duplicate operations."""
    cur.execute("INSERT INTO talks (title, emeritus, conference) VALUES (?, ?, ?)", (title, emeritus, conference_id))
    return cur.lastrowid


def insert_data(cur: sqlite3.Cursor, row: pd.Series, topics: list[str] | None = None) -> None:
    """Insert a single talk's data into the database.

    Args:
        cur: Database cursor
        row: Pandas Series containing talk data
        topics: Optional list of topics to associate with the talk
    """

    # Get or create conference
    conference_id = get_or_create_conference(cur, row.year, row.season)

    # Get or create organization and calling
    calling_obj = Calling(row.calling)
    calling_id = None
    if calling_obj:
        org_id = get_or_create_organization(cur, calling_obj.organization)
        calling_id = get_or_create_calling(cur, calling_obj.name, org_id, calling_obj.rank)
    else:
        logger.warning(f"Talk has no calling: {row.title} ({row.year} {row.season})")

    # Get or create talk
    emeritus = 1 if calling_obj and calling_obj.emeritus else 0
    talk_id = get_or_create_talk(cur, row.title, conference_id, emeritus)

    # Insert relationships (these don't have UNIQUE constraints in the same way)

    # Get or create speaker
    speaker_name = get_speaker(row.speaker)
    if speaker_name:
        speaker_id = get_or_create_speaker(cur, speaker_name)
        cur.execute("INSERT OR IGNORE INTO talk_speakers (talk, speaker) VALUES (?, ?)", (talk_id, speaker_id))
    else:
        logger.warning(f"Talk has no speaker: {row.title} ({row.year} {row.season})")

    if calling_id:
        cur.execute("INSERT OR IGNORE INTO talk_callings (talk, calling) VALUES (?, ?)", (talk_id, calling_id))

    # Insert talk text
    cur.execute("INSERT OR IGNORE INTO talk_texts (talk, text) VALUES (?, ?)", (talk_id, row.talk))

    # Insert talk URL
    cur.execute("INSERT OR IGNORE INTO talk_urls (talk, url, kind) VALUES (?, ?, 'text')", (talk_id, row.url))

    # Insert topics if provided
    if topics:
        for topic in topics:
            if topic.strip():  # Only insert non-empty topics
                cur.execute("INSERT OR IGNORE INTO talk_topics (talk, name) VALUES (?, ?)", (talk_id, topic.strip()))


def save_sql(
    con: sqlite3.Connection, cur: sqlite3.Cursor, conference_df: pd.DataFrame, topics_df: pd.DataFrame | None = None
) -> None:
    """Save conference data to SQLite database.

    Args:
        con: Database connection
        cur: Database cursor
        conference_df: DataFrame containing talk data
        topics_df: Optional DataFrame with topics (must have same index as conference_df)
    """
    # Clear caches at the start to ensure fresh data for each run
    get_or_create_speaker.cache_clear()
    get_or_create_organization.cache_clear()
    get_or_create_calling.cache_clear()
    get_or_create_conference.cache_clear()
    get_or_create_talk.cache_clear()

    # Process each talk individually
    for idx, row in conference_df.iterrows():
        try:
            # Get topics for this talk if available
            topics = None
            if topics_df is not None and idx in topics_df.index:
                topics_raw = topics_df.loc[idx, "topics"]
                if pd.notna(topics_raw) and isinstance(topics_raw, list):
                    topics = [t for t in topics_raw if t and t.strip()]  # Filter out empty topics

            insert_data(cur, row, topics)
        except Exception as e:
            logger.error(f"Failed to save talk '{row.title}' ({row.year} {row.season}): {e}")
            continue

    con.commit()
    logger.debug("Database save operation completed")
