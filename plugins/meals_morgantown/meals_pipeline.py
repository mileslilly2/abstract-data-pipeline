from .crawl_sites import scrape_sources
from .verify_sources import merge_verifications
from .export_formats import to_json, to_docx, to_ics

def run(context):
    scraped = scrape_sources("data/ingest_sources.yaml")
    verified = merge_verifications(scraped, "data/verifications.csv")
    json_path = to_json(verified, "data/meals_index.json")
    to_docx(verified, "exports/Free_Meals_Morgantown.docx")
    to_ics(verified, "exports/meals_calendar.ics")
    context.log.info(f"✅ Published {len(verified)} meal listings to {json_path}")
