<<<<<<< HEAD
# Abstract Data Pipeline (ADP)

Reusable, pluggable Python framework for fetching, cleaning, and exporting datasets.

## Diagram
=======
Got it — here’s the same README but with a simple diagram section so someone scanning the repo immediately sees how the pieces fit together.

---

# Abstract Data Pipeline (ADP)

**Reusable, pluggable Python framework for fetching, cleaning, and exporting datasets** for dashboards, maps, and other applications.
Works with APIs, CSV/Excel, shapefiles, PDFs, and more.
Designed so you can swap in new data sources by adding a plugin — no forking required.

---

## ✨ Features

* **Source / Transform / Sink** primitives for clean separation of concerns
* **Plugin-based** — add new data sources as small packages
* **YAML-defined pipelines** — no hardcoding, easy to run in CI
* **Multiple output formats** — JSON, GeoJSON, CSV, Parquet, SQLite
* **Incremental runs** — store cursors / etags between executions
* **CLI + importable API** — run from the shell or inside Python

---

## 📦 Install

```bash
git clone https://github.com/you/abstract-data-pipeline
cd abstract-data-pipeline

# Core
pip install -e .

# Install example plugins
pip install -e plugins/adp_plugins_disaster
pip install -e plugins/adp_plugins_ice
```

---

## 🚀 Quickstart

Run a prebuilt pipeline from YAML:

```bash
python -m adp.cli run pipelines/weather_alerts.yaml
```

Or from Python:

```python
from adp.core.runner import run_pipeline
run_pipeline("pipelines/weather_alerts.yaml")
```

Outputs go to the `out/` folder by default (configurable in YAML).

---

## 📂 Repo Structure

```
abstract-data-pipeline/
├─ adp/                # Core framework (no heavy deps)
│  ├─ core/base.py     # ABCs: Source, Transform, Sink, Pipeline
│  ├─ core/runner.py   # Executes YAML pipelines
│  ├─ cli.py           # Typer CLI entry point
│  └─ registry.py      # Plugin discovery
├─ plugins/            # Example plugin packages
│  ├─ adp_plugins.disaster/
│  │  └─ weather_gov.py
│  ├─ adp_plugins.ice/
│  │  └─ ice_excel.py
├─ pipelines/          # YAML pipeline specs
│  ├─ weather_alerts.yaml
│  └─ ice_clean.yaml
├─ examples/           # Notebooks / integration guides
└─ tests/
```

---

## 🔍 How It Works
>>>>>>> origin-main

```mermaid
flowchart LR
    A[Source] --> B[Transform(s)]
    B --> C[Sink]
<<<<<<< HEAD

=======
    
>>>>>>> origin-main
    subgraph Pipeline
        direction LR
        A --> B --> C
    end
<<<<<<< HEAD
=======

    style A fill:#e6f7ff,stroke:#1890ff,stroke-width:2px
    style B fill:#fff7e6,stroke:#fa8c16,stroke-width:2px
    style C fill:#f6ffed,stroke:#52c41a,stroke-width:2px
```

* **Source**: fetches raw data from APIs, files, databases
* **Transform**: cleans, enriches, joins, normalizes records
* **Sink**: writes data to a target format (JSON, GeoJSON, CSV, Parquet, SQLite, etc.)
* **Pipeline**: orchestrates the steps, with config/state/logging

---

## 🧩 Writing a Plugin

1. Create a new package (in `plugins/` or separate repo).
2. Implement one or more of:

   * `Source` — fetches raw data
   * `Transform` — cleans/enriches rows
   * `Sink` — writes outputs
3. Register via `pyproject.toml` entry point:

```toml
[project.entry-points."adp.plugins"]
weather_gov = "adp_plugins.disaster.weather_gov:WeatherGovAlertsSource"
```

4. Use in YAML:

```yaml
source:
  class: adp_plugins.disaster.weather_gov:WeatherGovAlertsSource
  params:
    location: "39.485,-80.15"
    days_back: 7
```

---

## 🔗 Integrating With Other Projects

* **In a web app**: copy or symlink the `out/` folder to your app’s `public/data/`
* **In a data science workflow**: `import adp` and run pipelines inside notebooks
* **In CI**: schedule GitHub Actions to run nightly and push outputs to S3 or `gh-pages`

Example in a Next.js project:

```json
"scripts": {
  "fetch:data": "adp run ../abstract-data-pipeline/pipelines/weather_alerts.yaml && cp -r ../abstract-data-pipeline/out/weather ./public/data"
}
```

---

## 🛠 Example Pipelines

### Weather.gov Alerts → GeoJSON

`pipelines/weather_alerts.yaml`

```yaml
name: weather_alerts
outdir: out/weather
source:
  class: adp_plugins.disaster.weather_gov:WeatherGovAlertsSource
  params:
    location: "39.485,-80.15"
    days_back: 7
transforms:
  - class: adp_plugins.disaster.weather_gov:AlertsToFlatRecords
sink:
  class: adp_plugins.disaster.weather_gov:GeoJsonAlertsSink
  params:
    filename: alerts.geojson
```

### ICE Excel → CSV

`pipelines/ice_clean.yaml`

```yaml
name: ice_clean
outdir: out/ice
source:
  class: adp_plugins.ice.ice_excel:LocalExcelFiles
  params:
    folder: "/content/raw2/ice_release_jul2025"
transforms:
  - class: adp_plugins.ice.ice_excel:DetectHeaderAndRead
sink:
  class: adp_plugins.ice.ice_excel:CsvSink
  params:
    filename: ice_clean.csv
```

---

## 📅 CI / Scheduled Runs

See `.github/workflows/nightly.yml` for an example GitHub Actions workflow that runs pipelines on a schedule and uploads artifacts.

---

## 📜 License


>>>>>>> origin-main
