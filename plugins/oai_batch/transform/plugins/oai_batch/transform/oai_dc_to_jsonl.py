import glob
import xml.etree.ElementTree as ET
import json
import uuid
import os

NAMESPACE = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/"
}

RAW_DIR = "data/wvu_oai/oai_dc/raw"
OUT_PATH = "data/wvu_oai/oai_dc/processed/records.jsonl"

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

def text(node):
    return node.text.strip() if node is not None and node.text else ""

with open(OUT_PATH, "w", encoding="utf-8") as out:
    for path in glob.glob(f"{RAW_DIR}/*.xml"):
        root = ET.parse(path).getroot()

        for record in root.findall(".//oai:record", NAMESPACE):
            out.write(json.dumps({
                "doc_id": str(uuid.uuid4()),
                "source": "wvu_oai",
                "collection": "oai_dc",
                "title": text(record.find(".//dc:title", NAMESPACE)),
                "description": text(record.find(".//dc:description", NAMESPACE)),
                "creators": [c.text for c in record.findall(".//dc:creator", NAMESPACE)],
                "subjects": [s.text for s in record.findall(".//dc:subject", NAMESPACE)],
                "link": text(record.find(".//dc:identifier", NAMESPACE)),
                "raw_source_file": os.path.basename(path)
            }) + "\n")
