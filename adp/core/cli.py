# adp/cli.py
"""Tiny CLI for ADP pipelines."""

from __future__ import annotations
import typer
from pathlib import Path
from typing import List

from .core.runner import run_pipeline
from .registry import list_registered_plugins

app = typer.Typer(help="ADP (Abstract Data Pipeline) CLI")


@app.command("run")
def run(spec: str, workdir: str = "."):
    """
    Run a pipeline YAML spec.

    Example:
        adp run pipelines/weather_alerts.yaml --workdir .
    """
    spec_path = Path(spec)
    if not spec_path.exists():
        raise typer.Exit(code=2, message=f"Spec not found: {spec}")
    run_pipeline(spec_path, workdir=workdir)


@app.command("list-plugins")
def list_plugins() -> None:
    """
    List discovered plugins registered under the `adp.plugins` entry point group.
    """
    plugins = list_registered_plugins()
    if not plugins:
        typer.echo("No adp.plugins entry points discovered.")
        raise typer.Exit()
    for name, ep in plugins:
        typer.echo(f"{name} -> {ep}")


@app.command("plugins")
def plugins_json() -> None:
    """Print plugin list as JSON-like output (helpful for scripts)."""
    import json
    plugins = list_registered_plugins()
    mapping = {name: ep for name, ep in plugins}
    typer.echo(json.dumps(mapping, indent=2))


if __name__ == "__main__":
    app()
