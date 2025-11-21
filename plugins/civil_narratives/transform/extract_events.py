"""
Extract hybrid events from narrative text.
Currently: placeholder deterministic splitter.
Later: implement R1 / R2 / R3 extraction strategy.
"""

from ..models import Event

def extract_events(text: str):
    events = []

    # SIMPLE STUB: break on sentences
    sentences = [s.strip() for s in text.split(".") if s.strip()]

    for i, s in enumerate(sentences):
        events.append(Event(
            event_id=f"E{i+1}",
            actor="unknown",
            action="unknown_action",
            details={"content_text": s}
        ))

    return events
