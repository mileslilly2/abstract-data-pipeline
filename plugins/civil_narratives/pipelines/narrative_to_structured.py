"""
Pipeline: raw narrative → structured CivilNarrative
"""

from ..transform.pipeline import transform

def run(text):
    return transform(text)
