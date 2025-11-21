"""
Map events to legal element support.
"""

from ..models import ElementSupportItem

def fill_elements(issue_hypotheses, events):
    element_map = {}

    for ih in issue_hypotheses:
        key = ih.label
        element_map[key] = []

        # Stub: all elements unknown
        element_map[key].append(ElementSupportItem(
            name="unwelcome_conduct",
            status="unknown",
            evidence_event_ids=[]
        ))

        element_map[key].append(ElementSupportItem(
            name="causation",
            status="unknown",
            evidence_event_ids=[]
        ))

    return element_map
