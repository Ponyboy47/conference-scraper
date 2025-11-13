"""Database operations for storing conference data."""

import functools
import logging
import sqlite3
from pathlib import Path

import pandas as pd
from groq import Groq

from .models import Calling, get_speaker

logger = logging.getLogger(__name__)

# Current schema version - increment this when making schema changes
CURRENT_SCHEMA_VERSION = 1


def get_schema_version(cur: sqlite3.Cursor) -> int:
    """Get the current schema version from the database."""
    try:
        cur.execute("SELECT version FROM schema_versions ORDER BY version DESC LIMIT 1")
        result = cur.fetchone()
        return result[0] if result else 0
    except sqlite3.OperationalError:
        # schema_versions table doesn't exist yet
        return 0


def set_schema_version(cur: sqlite3.Cursor, version: int) -> None:
    """Set the current schema version in the database."""
    cur.execute("INSERT OR REPLACE INTO schema_versions (id, version) VALUES (1, ?)", (version,))


def migrate_to_v1(cur: sqlite3.Cursor) -> None:
    """Apply migration to version 1: Initial schema."""
    logger.info("Applying migration to version 1 (initial schema)")

    # Create all tables with IF NOT EXISTS to avoid errors if they already exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS speakers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS organizations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS callings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            organization INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            UNIQUE(name, organization) ON CONFLICT IGNORE,
            FOREIGN KEY(organization) REFERENCES organizations
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS conferences(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year NOT NULL,
            season TEXT NOT NULL,
            UNIQUE(year, season) ON CONFLICT IGNORE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            emeritus INTEGER NOT NULL DEFAULT 0,
            conference INTEGER NOT NULL,
            UNIQUE(title, conference) ON CONFLICT IGNORE,
            FOREIGN KEY(conference) REFERENCES conferences
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talk_speakers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            speaker INTEGER NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
            FOREIGN KEY(speaker) REFERENCES speakers
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talk_callings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            calling INTEGER NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
            FOREIGN KEY(calling) REFERENCES callings
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talk_texts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER UNIQUE NOT NULL,
            text TEXT NOT NULL,
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talk_urls(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            url TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind in ('audio', 'video', 'text')),
            UNIQUE(talk, url) ON CONFLICT IGNORE
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talk_topics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talk INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE(talk, name) ON CONFLICT IGNORE
            FOREIGN KEY(talk) REFERENCES talks
        )
    """)


def apply_migrations(cur: sqlite3.Cursor, target_version: int = CURRENT_SCHEMA_VERSION) -> None:
    """Apply database migrations to bring schema up to target version."""
    current_version = get_schema_version(cur)
    logger.info(f"Current schema version: {current_version}, Target version: {target_version}")

    if current_version < target_version:
        logger.info(f"Applying migrations from version {current_version} to {target_version}")

        # Apply migrations incrementally
        for version in range(current_version + 1, target_version + 1):
            if version == 1:
                migrate_to_v1(cur)
                set_schema_version(cur, version)
                logger.info(f"Successfully migrated to version {version}")
            # Add future migrations here as elif blocks:
            # elif version == 2:
            #     migrate_to_v2(cur)
            #     set_schema_version(cur, version)
            else:
                raise ValueError(f"No migration available for version {version}")
    else:
        logger.debug(f"Schema is already at version {current_version}")


def setup_sql() -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Initialize SQLite database with proper schema migration support."""
    db_path = Path("conference_talks.db")

    # Create database file if it doesn't exist
    db_exists = db_path.exists()
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Create schema_versions table first (must be done before migrations)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_versions(
            id INTEGER PRIMARY KEY,
            version INTEGER NOT NULL,
            migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Apply any necessary migrations
    apply_migrations(cur, CURRENT_SCHEMA_VERSION)

    if not db_exists:
        logger.info("Created new database with schema version 1")
    else:
        logger.info(f"Database ready (schema version {get_schema_version(cur)})")

    return con, cur


@functools.cache
def get_or_create_speaker(cur: sqlite3.Cursor, name: str) -> int:
    """Get or create a speaker and return their ID. Cached to avoid duplicate operations."""
    # First try to find existing speaker
    cur.execute("SELECT id FROM speakers WHERE name = ?", (name,))
    result = cur.fetchone()
    if result:
        return result[0]

    # If not found, insert new speaker
    cur.execute("INSERT INTO speakers (name) VALUES (?)", (name,))
    return cur.lastrowid


@functools.cache
def get_or_create_organization(cur: sqlite3.Cursor, name: str) -> int:
    """Get or create an organization and return their ID. Cached to avoid duplicate operations."""
    # First try to find existing organization
    cur.execute("SELECT id FROM organizations WHERE name = ?", (name,))
    result = cur.fetchone()
    if result:
        return result[0]

    # If not found, insert new organization
    cur.execute("INSERT INTO organizations (name) VALUES (?)", (name,))
    return cur.lastrowid


@functools.cache
def get_or_create_calling(cur: sqlite3.Cursor, name: str, organization_id: int, rank: int) -> int:
    """Get or create a calling and return their ID. Cached to avoid duplicate operations."""
    # First try to find existing calling
    cur.execute("SELECT id FROM callings WHERE name = ? AND organization = ?", (name, organization_id))
    result = cur.fetchone()
    if result:
        return result[0]

    # If not found, insert new calling
    cur.execute("INSERT INTO callings (name, organization, rank) VALUES (?, ?, ?)", (name, organization_id, rank))
    return cur.lastrowid


@functools.cache
def get_or_create_conference(cur: sqlite3.Cursor, year: int, season: str) -> int:
    """Get or create a conference and return their ID. Cached to avoid duplicate operations."""
    # First try to find existing conference
    cur.execute("SELECT id FROM conferences WHERE year = ? AND season = ?", (year, season))
    result = cur.fetchone()
    if result:
        return result[0]

    # If not found, insert new conference
    cur.execute("INSERT INTO conferences (year, season) VALUES (?, ?)", (year, season))
    return cur.lastrowid


@functools.cache
def get_or_create_talk(cur: sqlite3.Cursor, title: str, conference_id: int, emeritus: int) -> tuple[int, bool]:
    """Get or create a talk and return their ID and whether it was newly created. Cached to avoid duplicate operations.

    Returns:
        tuple: (talk_id, is_new) where is_new is True if the talk was just inserted
    """
    # First try to find existing talk
    cur.execute("SELECT id FROM talks WHERE title = ? AND conference = ?", (title, conference_id))
    result = cur.fetchone()
    if result:
        return result[0], False  # Talk exists, not new

    # If not found, insert new talk
    cur.execute("INSERT INTO talks (title, emeritus, conference) VALUES (?, ?, ?)", (title, emeritus, conference_id))
    return cur.lastrowid, True  # Talk is new


def insert_data(cur: sqlite3.Cursor, row: pd.Series, topics: list[str] | None = None) -> bool:
    """Insert a single talk's data into the database.

    Args:
        cur: Database cursor
        row: Pandas Series containing talk data
        topics: Optional list of topics to associate with the talk

    Returns:
        bool: True if the talk was newly inserted, False if it already existed
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
    talk_id, is_new_talk = get_or_create_talk(cur, row.title, conference_id, emeritus)

    # Only insert relationships if this is a new talk or they don't exist
    # (existing talks might already have these relationships)

    # Get or create speaker
    speaker_name = get_speaker(row.speaker)
    if speaker_name:
        speaker_id = get_or_create_speaker(cur, speaker_name)
        # Only insert if relationship doesn't exist
        cur.execute("INSERT OR IGNORE INTO talk_speakers (talk, speaker) VALUES (?, ?)", (talk_id, speaker_id))
    elif is_new_talk:
        logger.warning(f"Talk has no speaker: {row.title} ({row.year} {row.season})")

    if calling_id:
        cur.execute("INSERT OR IGNORE INTO talk_callings (talk, calling) VALUES (?, ?)", (talk_id, calling_id))

    # Insert talk text (only if it's a new talk)
    if is_new_talk:
        cur.execute("INSERT INTO talk_texts (talk, text) VALUES (?, ?)", (talk_id, row.talk))

    # Insert talk URL (only if it's a new talk)
    if is_new_talk:
        cur.execute("INSERT INTO talk_urls (talk, url, kind) VALUES (?, ?, 'text')", (talk_id, row.url))

    # Insert topics if provided (only for new talks)
    if topics and is_new_talk:
        for topic in topics:
            if topic.strip():  # Only insert non-empty topics
                cur.execute("INSERT OR IGNORE INTO talk_topics (talk, name) VALUES (?, ?)", (talk_id, topic.strip()))

    return is_new_talk


def save_sql(
    con: sqlite3.Connection, cur: sqlite3.Cursor, conference_df: pd.DataFrame, topics_df: pd.DataFrame | None = None
) -> list[int]:
    """Save conference data to SQLite database.

    Args:
        con: Database connection
        cur: Database cursor
        conference_df: DataFrame containing talk data
        topics_df: Optional DataFrame with topics (must have same index as conference_df)

    Returns:
        list: Indices of talks that were newly inserted (for topic extraction)
    """
    logger = logging.getLogger(__name__)

    # Clear caches at the start to ensure fresh data for each run
    get_or_create_speaker.cache_clear()
    get_or_create_organization.cache_clear()
    get_or_create_calling.cache_clear()
    get_or_create_conference.cache_clear()
    get_or_create_talk.cache_clear()

    # Track which talks are new and need topic extraction
    new_talks_indices = []

    # First pass: insert all data and collect which talks are new
    for idx, row in conference_df.iterrows():
        try:
            # Get topics for this talk if available (but only use them for new talks)
            topics = None
            if topics_df is not None and idx in topics_df.index:
                topics_raw = topics_df.loc[idx, "topics"]
                if pd.notna(topics_raw) and isinstance(topics_raw, list):
                    topics = [t for t in topics_raw if t and t.strip()]  # Filter out empty topics

            is_new = insert_data(cur, row, topics if topics_df is not None else None)
            if is_new:
                new_talks_indices.append(idx)
        except Exception as e:
            logger.error(f"Failed to save talk '{row.title}' ({row.year} {row.season}): {e}")
            continue

    con.commit()
    logger.debug("Database save operation completed")

    return new_talks_indices


def update_talk_topics(cur: sqlite3.Cursor, conference_df: pd.DataFrame, topics_df: pd.DataFrame) -> None:
    """Update topics for existing talks in the database.

    Args:
        cur: Database cursor
        conference_df: DataFrame containing talk data (subset of talks to update)
        topics_df: DataFrame with topics for those talks
    """
    logger = logging.getLogger(__name__)

    for idx in conference_df.index:
        if idx in topics_df.index:
            row = conference_df.loc[idx]
            topics = topics_df.loc[idx, "topics"]

            if pd.notna(topics) and isinstance(topics, list) and topics:
                # Find the talk ID
                conference_id_result = cur.execute(
                    "SELECT id FROM conferences WHERE year = ? AND season = ?", (row.year, row.season)
                ).fetchone()

                if conference_id_result:
                    conference_id = conference_id_result[0]
                    talk_result = cur.execute(
                        "SELECT id FROM talks WHERE title = ? AND conference = ?", (row.title, conference_id)
                    ).fetchone()

                    if talk_result:
                        talk_id = talk_result[0]
                        # Insert topics
                        for topic in topics:
                            if topic.strip():
                                cur.execute(
                                    "INSERT OR IGNORE INTO talk_topics (talk, name) VALUES (?, ?)",
                                    (talk_id, topic.strip()),
                                )

    logger.debug("Topics updated for existing talks")


def insert_data_with_topics(cur: sqlite3.Cursor, row: pd.Series, topic_client: Groq | None = None) -> bool:
    """Insert a single talk's data into the database, optionally extracting topics if new.

    Args:
        cur: Database cursor
        row: Pandas Series containing talk data
        topic_client: Groq client for topic extraction (if None, skips topic extraction)

    Returns:
        bool: True if the talk was newly inserted, False if it already existed
    """
    logger = logging.getLogger(__name__)

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
    talk_id, is_new_talk = get_or_create_talk(cur, row.title, conference_id, emeritus)

    # Only insert relationships and content if this is a new talk
    if is_new_talk:
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
        cur.execute("INSERT INTO talk_texts (talk, text) VALUES (?, ?)", (talk_id, row.talk))

        # Insert talk URL
        cur.execute("INSERT INTO talk_urls (talk, url, kind) VALUES (?, ?, 'text')", (talk_id, row.url))

        # Extract and insert topics if client is provided
        if topic_client and pd.notna(row.talk) and row.talk.strip():
            try:
                from . import topic_extractor

                topics = topic_extractor.extract_topics_groq(row.talk.strip(), topic_client)
                for topic in topics:
                    if topic.strip():
                        cur.execute(
                            "INSERT OR IGNORE INTO talk_topics (talk, name) VALUES (?, ?)", (talk_id, topic.strip())
                        )
                logger.debug(f"Extracted {len(topics)} topics for talk: {row.title}")
            except Exception as e:
                logger.error(f"Failed to extract topics for talk '{row.title}': {e}")
                # Continue anyway - the talk is saved even if topic extraction fails

    return is_new_talk
