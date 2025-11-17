"""Data models and classes for conference data."""

import re
import unicodedata

calling_re = re.compile(
    r"(?P<emeritus>(recently\s)?((released|former)\s)?((as|member\sof\sthe)\s)?)(?P<calling>[\w,\s()\d-]+)$",
    flags=re.I | re.U,
)
org_re = re.compile(r"[\w\s]+(,\s|\sin\sthe\s)(?P<org>[\w\s-]+)$", flags=re.I | re.U)

speaker_re = re.compile(
    r"((Presented\s)?by\s)(?P<office>(President|Elder|Brother|Sister|Bishop))?\s?(?P<speaker>[^\s][\w,.\s()-]+)$",
    flags=re.I | re.U,
)


class Calling:
    """Represents a church calling with organization and rank information."""

    def __init__(self, full_calling: str | None):
        if not full_calling:
            self.name = "Unknown"
            self.organization = "Unknown"
            self.rank = 1000
            self.org_rank = 1000
            self.emeritus = False
            return

        matches = calling_re.search(full_calling)
        if not matches:
            raise ValueError(f"Unsupported calling: {full_calling}")

        self.name = matches.group("calling").strip().title()
        self.organization, self.rank, self.org_rank = Calling.get_org_and_rank(self.name)
        self.emeritus = len(matches.group("emeritus").strip()) > 0

    def __bool__(self) -> bool:
        return self.rank < 1000

    @staticmethod
    def get_org_and_rank(calling: str) -> tuple[str, int, int]:
        org = "Local"
        rank = 99
        org_rank = 99
        lowered = calling.lower()
        if "president of the church" in lowered:
            org = "First Presidency"
            rank = 1
            org_rank = 1
        elif "first presidency" in lowered:
            org = "First Presidency"
            rank = 2
            org_rank = 1
        elif "of the twelve" in lowered:
            org = "Quorum of the Twelve Apostles"
            rank = 3
            org_rank = 2
        elif "of the seventy" in lowered:
            org = "Quorum of the Seventy"
            rank = 4
            org_rank = 3
        elif "presiding bishop" in lowered:
            org = "Presiding Bishopric"
            rank = 5
            org_rank = 4
        elif lowered.endswith("general presidency"):
            if "young men" in lowered:
                rank = 6
                org_rank = 5
            elif "sunday school in lowered":
                rank = 7
                org_rank = 6
            elif "relief society" in lowered:
                rank = 8
                org_rank = 7
            elif "young women" in lowered:
                rank = 9
                org_rank = 8
            elif "primary" in lowered:
                rank = 10
                org_rank = 9
            else:
                raise ValueError(f"Unsupported calling for organization: {calling}")
            org = org_re.search(lowered).group("org").strip().title()
        elif lowered.endswith("general president"):
            if "young men" in lowered:
                org = "Young Men General Presidency"
                rank = 6
                org_rank = 5
            elif "sunday school in lowered":
                org = "Sunday School General Presidency"
                rank = 7
                org_rank = 6
            elif "relief society" in lowered:
                org = "Relief Society General Presidency"
                rank = 8
                org_rank = 7
            elif "young women" in lowered:
                org = "Young Women General Presidency"
                rank = 9
                org_rank = 8
            elif "primary" in lowered:
                org = "Primary General Presidency"
                rank = 10
                org_rank = 9
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
        return org, rank, org_rank


def get_speaker(full_speaker: str | None) -> str | None:
    """Extract clean speaker name from full speaker string."""
    if not full_speaker:
        return None

    speaker = full_speaker.strip()
    match = speaker_re.search(unicodedata.normalize("NFC", speaker))
    if match:
        speaker = match.group("speaker").strip()
    else:
        print(f"Failed speaker match: {speaker}")
    return unicodedata.normalize("NFD", speaker)
