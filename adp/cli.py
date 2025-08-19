"""Shim that re-exports the Typer CLI defined in adp.core.cli."""
from adp.core.cli import app

# Allow  `python -m adp.cli …`
if __name__ == "__main__":
    app()
