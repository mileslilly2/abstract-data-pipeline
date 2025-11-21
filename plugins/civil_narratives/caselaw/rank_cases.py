"""
Rank cases by similarity.
"""

import numpy as np

def rank(event_vecs, case_vec):
    return float(np.dot(event_vecs, case_vec).max())
