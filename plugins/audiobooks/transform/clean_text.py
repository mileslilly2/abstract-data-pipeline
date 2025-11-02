#!/usr/bin/env python3
# transform_clean_text.py
# Transform 1: parse and clean Gutenberg HTML

from bs4 import BeautifulSoup
import re
from pathlib import Path

def extract_story_text(html_path: Path):
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    # Try to grab title and author
    title_tag = soup.find("title") or soup.find("h1")
    title = title_tag.get_text(" ", strip=True) if title_tag else "Untitled"

    author_tag = soup.find(string=re.compile(r"Author", re.I))
    author = author_tag.strip() if author_tag else "Unknown Author"

    # Remove Project Gutenberg license/boilerplate
    text = re.sub(r"\*{3}\s*START OF.+?\*{3}", "", html, flags=re.DOTALL)
    text = re.sub(r"\*{3}\s*END OF.+?\*{3}", "", text, flags=re.DOTALL)

    # Extract main paragraphs
    clean = BeautifulSoup(text, "lxml").get_text("\n", strip=True)
    clean = re.sub(r"\s{2,}", "\n", clean)
    return {"title": title, "author": author, "text": clean}

if __name__ == "__main__":
    from pathlib import Path
    story = extract_story_text(Path("data/gutenberg_raw/1342.html"))
    print(story["title"], story["author"], len(story["text"]))
