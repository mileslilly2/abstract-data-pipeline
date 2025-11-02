#!/usr/bin/env python3
import io, re, tarfile, zipfile, requests, time, tempfile
import xml.etree.ElementTree as ET

RDF_FEED_URL = "https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip"

def stream_rdf_entries(limit=50, language="en", topic=None):
    """
    Stream-parse Gutenberg RDF feed directly from the zip archive.
    Yields dicts: {"id": int, "title": str, "author": str, "subjects": [str], "rights": str}
    """
    print(f"[{time.strftime('%H:%M:%S')}] 📥 Downloading RDF feed header…")

    # Download to temp file for true streaming read
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    with requests.get(RDF_FEED_URL, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            tmp.write(chunk)
            total += len(chunk)
            if total > 50_000_000:  # stop early (first ~50MB)
                break
    tmp.flush()
    tmp.seek(0)

    print(f"[{time.strftime('%H:%M:%S')}] 🔍 Parsing first portion of RDF feed (~{total//1e6:.1f} MB)…")

    with zipfile.ZipFile(tmp.name) as z:
        tar_name = next((n for n in z.namelist() if n.endswith(".tar")), None)
        if tar_name is None:
            raise RuntimeError("No .tar found inside RDF zip!")

        with z.open(tar_name) as tar_stream:
            with tarfile.open(fileobj=tar_stream, mode="r|") as tf:
                ns = {
                    "dcterms": "http://purl.org/dc/terms/",
                    "pgterms": "http://www.gutenberg.org/2009/pgterms/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                }
                count = 0
                for member in tf:
                    if not member.name.endswith(".rdf"):
                        continue
                    f = tf.extractfile(member)
                    if not f:
                        continue
                    try:
                        tree = ET.parse(f)
                        ebook = tree.find(".//pgterms:ebook", ns)
                        if ebook is None:
                            continue

                        about = ebook.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "")
                        m = re.search(r"ebooks/(\d+)", about)
                        if not m:
                            continue
                        book_id = int(m.group(1))

                        title_el = ebook.find("dcterms:title", ns)
                        title = title_el.text.strip() if title_el is not None else "Untitled"

                        author_el = ebook.find(".//pgterms:agent/pgterms:name", ns)
                        author = author_el.text.strip() if author_el is not None else "Unknown"

                        subjects = [
                            s.text.strip()
                            for s in ebook.findall(".//dcterms:subject/rdf:Description/rdf:value", ns)
                            if s.text
                        ]
                        rights_el = ebook.find("dcterms:rights", ns)
                        rights = rights_el.text.strip() if rights_el is not None else ""

                        # Filter by language/topic
                        langs = [
                            l.text for l in ebook.findall(".//dcterms:language/rdf:Description/rdf:value", ns)
                            if l.text
                        ]
                        if language and not any(language in l for l in langs):
                            continue
                        if topic and not any(topic.lower() in s.lower() for s in subjects):
                            continue

                        yield {
                            "id": book_id,
                            "title": title,
                            "author": author,
                            "subjects": subjects,
                            "language": langs,
                            "rights": rights,
                        }

                        count += 1
                        if limit and count >= limit:
                            break
                    except Exception:
                        continue

if __name__ == "__main__":
    for i, book in enumerate(stream_rdf_entries(limit=10, language="en")):
        print(f"{i+1:02d}. {book['id']} — {book['title']} by {book['author']}")
