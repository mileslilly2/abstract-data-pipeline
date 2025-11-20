"""
End-to-end transformation: raw text → CivilNarrative
"""

from ..models import CivilNarrative, Actor
from .extract_events import extract_events
from .classify_issues import classify_issues
from .fill_elements import fill_elements

def transform(raw_text: str) -> CivilNarrative:
    events = extract_events(raw_text)
    issues = classify_issues(events)
    elements = fill_elements(issues, events)

    return CivilNarrative(
        doc_id="doc001",
        actors=[Actor(actor_id="A1", role="unknown")],
        events=events,
        issue_hypotheses=issues,
        element_support=elements
    )
