from harvest import harvest_batches

PLUGIN_NAME = "wvu_oai"
SOURCE_NAME = "oai_dc"

if __name__ == "__main__":
    harvest_batches(
        base_url="https://researchrepository.wvu.edu/do/oai/",
        metadata_prefix="oai_dc",
        out_dir=f"data/{PLUGIN_NAME}/{SOURCE_NAME}/raw",
        max_batches=None,
    )
