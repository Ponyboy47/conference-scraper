"""Data models and classes for conference data."""

import re

calling_re = re.compile(
    r"(?P<emeritus>(recently\s)?((released|former)\s)?((as|member\sof\sthe)\s)?)(?P<calling>[a-zA-Z,\s()0-9-]+)$",
    flags=re.I,
)
org_re = re.compile(r"[a-zA-Z\s]+(,\s|\sin\sthe\s)(?P<org>[a-zA-Z\s-]+)$", flags=re.I)

speaker_re = re.compile(
    r"((Presented\s)?by\s)(?P<office>(President|Elder|Brother|Sister|Bishop))?\s?(?P<speaker>[^\s][a-zA-Z,.\s-]+)$",
    flags=re.I,
)


class Calling:
    """Represents a church calling with organization and rank information."""

    def __init__(self, full_calling: str | None):
        if not full_calling:
            self.name = "Unknown"
            self.organization = "Unknown"
            self.rank = 1000
            self.emeritus = False
            return

        matches = calling_re.search(full_calling)
        if not matches:
            raise ValueError(f"Unsupported calling: {full_calling}")

        self.name = matches.group("calling").strip()
        self.organization, self.rank = Calling.get_org_and_rank(self.name)
        self.emeritus = len(matches.group("emeritus").strip()) > 0

    def __bool__(self) -> bool:
        return self.rank < 1000

    @staticmethod
    def get_org_and_rank(calling: str) -> tuple[str, int]:
        org = "Local"
        rank = 99
        lowered = calling.lower()
        if "president of the church" in lowered:
            org = "First Presidency"
            rank = 0
        elif "first presidency" in lowered:
            org = "First Presidency"
            rank = 1
        elif "of the twelve" in lowered:
            org = "Quorum of the Twelve Apostles"
            rank = 2
        elif "of the seventy" in lowered:
            org = "Quorum of the Seventy"
            rank = 3
        elif "presiding bishop" in lowered:
            org = "Presiding Bishopric"
            rank = 4
        elif lowered.endswith("general presidency"):
            if "young men" in lowered:
                rank = 5
            elif "sunday school in lowered":
                rank = 6
            elif "relief society" in lowered:
                rank = 7
            elif "young women" in lowered:
                rank = 8
            elif "primary" in lowered:
                rank = 9
            else:
                raise ValueError(f"Unsupported calling for organization: {calling}")
            org = org_re.search(lowered).group("org").strip().title()
        elif lowered.endswith("general president"):
            if "young men" in lowered:
                org = "Young Men General Presidency"
                rank = 5
            elif "sunday school in lowered":
                org = "Sunday School General Presidency"
                rank = 6
            elif "relief society" in lowered:
                org = "Relief Society General Presidency"
                rank = 7
            elif "young women" in lowered:
                org = "Young Women General Presidency"
                rank = 8
            elif "primary" in lowered:
                org = "Primary General Presidency"
                rank = 9
            else:
                raise ValueError(f"Unsupported calling for organization: {calling}")
        elif any(
            map(
                lambda field: field in lowered,
                [
                    "church audit committee",
                    "church leadership committee",
                ],
            )
        ):
            org = calling.title()
        return org, rank


def get_speaker(full_speaker: str | None) -> str | None:
    """Extract clean speaker name from full speaker string."""
    if not full_speaker:
        return None

    speaker = full_speaker.strip()
    match = speaker_re.search(speaker)
    if match:
        speaker = match.group("speaker").strip()
    return speaker
