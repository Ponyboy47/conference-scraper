"""Topic extraction functionality using Groq API with rate limiting."""

import logging
import time
from typing import List

import groq
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Rate limiting: Usually 30 requests per minute = 2 seconds between requests
RATE_LIMIT_SECONDS = 3  # Slightly over 2 to be safe


def extract_topics_groq(text: str, client: groq.Groq) -> List[str]:
    """Extract 3-10 main topics from text using Groq API with rate limiting.

    Args:
        text: The speech text to analyze
        client: Groq API client instance

    Returns:
        List of topic strings (raises exception if extraction fails)

    Raises:
        Exception: If topic extraction fails for any reason
    """
    if not text.strip():
        return []

    # Highly optimized prompt for minimal tokens while maximizing clarity
    # Truncate text to first 4000 chars to save tokens (most talks have key themes early)
    truncated_text = text[:4000] if len(text) > 4000 else text

    prompt = f"""
Extract 3-5 main topics from this General Conference talk of the Church of Jesus Christ of Latter-day Saints.
Return only comma-separated topics, no explanations or other text:

{truncated_text}
"""

    # Don't catch exceptions - let them propagate to caller
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",  # Fast and has higher limits
        # model="llama-3.3-70b-versatile",  # Best quality for topic extraction
        # model="meta-llama/llama-4-maverick-17b-128e-instruct",  # Last fallback
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,  # Low temperature for consistent results
        max_tokens=100,  # Enough for 3-5 topics
        top_p=0.9,
    )

    topics_str = response.choices[0].message.content.strip()

    # Parse comma-separated topics and clean them
    topics = []
    for topic in topics_str.split(","):
        topic = topic.strip()
        # Remove common prefixes/suffixes that might appear
        topic = topic.strip("\"'").lstrip("•-• ").rstrip(".")
        if topic and len(topic) > 2:  # Filter out very short fragments
            topics.append(topic)

    topic_count = len(topics)
    if topic_count < 3 or topic_count > 5:
        logger.warning(f"Extracted {topic_count} topics, expected 3-5")

    logger.debug(f"Extracted {topic_count} topics: {topics}")
    return topics


def extract_topics(texts: List[str], client: groq.Groq) -> List[List[str]]:
    """Extract topics for multiple texts with rate limiting.

    Args:
        texts: List of text strings to analyze
        client: Groq API client instance

    Returns:
        List of topic lists, one per input text
    """
    results = []
    total_texts = len(texts)

    logger.info(f"Starting topic extraction for {total_texts} texts")

    # Use tqdm for nice progress bar
    with tqdm(total=total_texts, desc="Extracting topics", unit="talks") as pbar:
        for i, text in enumerate(texts):
            topics = extract_topics_groq(text, client)
            results.append(topics)
            pbar.update(1)

            # Try to avoid rate limiting
            time.sleep(RATE_LIMIT_SECONDS)

    logger.info("Topic extraction completed")
    return results
