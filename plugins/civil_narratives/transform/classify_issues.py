"""
Multi-label issue classifier placeholder.
"""

from ..models import IssueHypothesis

def classify_issues(events):
    # Example trivial classifier
    labels = []

    text_blob = " ".join([
        (ev.details.content_text or "")
        for ev in events
    ]).lower()

    if "harass" in text_blob:
        labels.append(IssueHypothesis(label="Harassment", confidence=0.70))

    if "retaliat" in text_blob:
        labels.append(IssueHypothesis(label="Retaliation", confidence=0.65))

    return labels
