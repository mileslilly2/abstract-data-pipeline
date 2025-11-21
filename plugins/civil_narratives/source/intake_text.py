"""
Intake raw text → normalized narrative string
"""

def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def normalize(text: str) -> str:
    text = text.replace("\r", "")
    return text.strip()

def intake_text_file(path: str) -> str:
    raw = load_text(path)
    return normalize(raw)
