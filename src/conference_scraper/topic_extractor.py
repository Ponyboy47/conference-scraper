"""Topic extraction functionality using Groq API with rate limiting."""

import logging
import time
from typing import List

import groq
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Rate limiting: Usually 30 requests per minute = 2 seconds between requests
RATE_LIMIT_SECONDS = 2.1  # Slightly over 2 to be safe


def extract_topics_groq(text: str, client: groq.Groq) -> List[str]:
    """Extract 3-10 main topics from text using Groq API with rate limiting.

    Args:
        text: The speech text to analyze
        client: Groq API client instance

    Returns:
        List of topic strings (empty list if extraction fails)
    """
    if not text.strip():
        return []

    # Highly optimized prompt for minimal tokens while maximizing clarity
    # Truncate text to first 4000 chars to save tokens (most talks have key themes early)
    truncated_text = text[:4000] if len(text) > 4000 else text

    prompt = f"""
Extract 3-10 main topics from this LDS General Conference talk. Return only comma-separated topics, no explanations:

{truncated_text}

Topics:"""

    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",  # Fast and has higher limits
            # model="llama-3.3-70b-versatile",  # Best quality for topic extraction
            # model="meta-llama/llama-4-maverick-17b-128e-instruct",  # Last fallback
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

        topic_count = len(topics)
        if topic_count < 3 or topic_count > 10:
            logger.warning(f"Extracted {topic_count} topics, expected 3-10")

        logger.debug(f"Extracted {topic_count} topics: {topics}")
        return topics

    except Exception as e:
        logger.error(f"Failed to extract topics using Groq API: {e}")
        return []


def extract_topics_batch(texts: List[str], client: groq.Groq, batch_size: int = 10) -> List[List[str]]:
    """Extract topics for multiple texts with batch processing and rate limiting.

    Args:
        texts: List of text strings to analyze
        client: Groq API client instance
        batch_size: Number of texts to process before logging progress

    Returns:
        List of topic lists, one per input text
    """
    results = []
    total_texts = len(texts)

    logger.info(f"Starting topic extraction for {total_texts} texts")

    # Use tqdm for nice progress bar
    with tqdm(total=total_texts, desc="Extracting topics", unit="talk") as pbar:
        for i, text in enumerate(texts):
            topics = extract_topics_groq(text, client)
            results.append(topics)
            pbar.update(1)

            # Try to avoid rate limiting
            time.sleep(RATE_LIMIT_SECONDS)

    logger.info("Topic extraction completed")
    return results
