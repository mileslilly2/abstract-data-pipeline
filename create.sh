#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="abstract-data-pipeline"
if [ -d "$REPO_DIR" ]; then
  echo "Directory $REPO_DIR already exists. Exiting."
  exit 1
fi

mkdir -p "$REPO_DIR"
cd "$REPO_DIR"

echo "Initializing git repo..."
git init -q

echo "Creating directories..."
mkdir -p adp/core adp/io plugins/adp_plugins.disaster/adp_plugins/disaster plugins/adp_plugins.ice/adp_plugins/ice pipelines examples tests .github/workflows

################################################################################
# .gitignore
################################################################################
cat > .gitignore <<'EOF'
# Python
__pycache__/
*.py[cod]
*.pyo
*.egg-info/
dist/
build/
out/
.env
.env.*
.ipynb_checkpoints/

# OS
.DS_Store

# Colab / data
*.zip
*.parquet
*.geojson
/content/

# virtualenv
venv/
ENV/
.env/

# node
node_modules/
EOF

################################################################################
# pyproject.toml (minimal)
################################################################################
cat > pyproject.toml <<'EOF'
[build-system]
requires = ["setuptools", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "abstract-data-pipeline"
version = "0.0.1"
description = "Small Source/Transform/Sink pipeline framework (example stubs)."
authors = [{name="You", email="you@example.com"}]
readme = "README.md"
requires-python = ">=3.9"
dependencies = [
  "requests",
  "pandas",
  "pyyaml"
]
EOF

################################################################################
# README.md (with mermaid diagram)
################################################################################
cat > README.md <<'EOF'
# Abstract Data Pipeline (ADP)

Reusable, pluggable Python framework for fetching, cleaning, and exporting datasets.

## Diagram

```mermaid
flowchart LR
    A[Source] --> B[Transform(s)]
    B --> C[Sink]

    subgraph Pipeline
        direction LR
        A --> B --> C
    end
