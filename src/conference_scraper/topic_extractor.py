"""Topic extraction functionality using Groq API with rate limiting."""

import logging
import time
from typing import List

import groq

logger = logging.getLogger(__name__)

# Rate limiting: 30 requests per minute = 2 seconds between requests
RATE_LIMIT_SECONDS = 2.1  # Slightly over 2 to be safe
_last_request_time = 0


def _rate_limit():
    """Enforce rate limiting between API calls."""
    global _last_request_time
    current_time = time.time()
    time_since_last = current_time - _last_request_time

    if time_since_last < RATE_LIMIT_SECONDS:
        sleep_time = RATE_LIMIT_SECONDS - time_since_last
        logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)

    _last_request_time = time.time()


def extract_topics_groq(text: str, client: groq.Groq) -> List[str]:
    """Extract 3-10 main topics from text using Groq API with rate limiting.

    Args:
        text: The speech text to analyze
        api_key: Groq API key (if None, will try GROQ_API_KEY env var)

    Returns:
        List of topic strings (empty list if extraction fails)
    """
    if not text or not text.strip():
        return []

    # Enforce rate limiting
    _rate_limit()

    # Highly optimized prompt for minimal tokens while maximizing clarity
    # Truncate text to first 4000 chars to save tokens (most talks have key themes early)
    truncated_text = text[:4000] if len(text) > 4000 else text

    prompt = f"""
Extract 3-10 main topics from this LDS General Conference talk. Return only comma-separated topics, no explanations:

{truncated_text}

Topics:"""

    try:
        response = client.chat.completions.create(
            model="mixtral-8x7b-32768",  # Fast, good for topic extraction
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,  # Low temperature for consistent results
            max_tokens=100,  # Enough for 3-10 topics
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

        # Limit to 3-10 topics as requested
        topics = topics[:10] if len(topics) > 10 else topics

        if len(topics) < 3:
            logger.warning(f"Only extracted {len(topics)} topics, expected 3-10")

        logger.debug(f"Extracted {len(topics)} topics: {topics[:3]}...")
        return topics

    except Exception as e:
        logger.error(f"Failed to extract topics using Groq API: {e}")
        return []


def extract_topics_batch(texts: List[str], client: groq.Groq, batch_size: int = 10) -> List[List[str]]:
    """Extract topics for multiple texts with batch processing and rate limiting.

    Args:
        texts: List of text strings to analyze
        api_key: Groq API key (if None, will try GROQ_API_KEY env var)
        batch_size: Number of texts to process before logging progress

    Returns:
        List of topic lists, one per input text
    """
    results = []
    total_texts = len(texts)

    logger.info(f"Starting topic extraction for {total_texts} texts")

    for i, text in enumerate(texts):
        if (i + 1) % batch_size == 0 or i == 0:
            logger.info(f"Processing text {i + 1}/{total_texts}")

        topics = extract_topics_groq(text, client)
        results.append(topics)

    logger.info("Topic extraction completed")
    return results
