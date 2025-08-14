import typer
from pathlib import Path
from .core.runner import run_pipeline

app = typer.Typer(no_args_is_help=True)

@app.command()
def run(spec: str, workdir: str = "."):
    """Run a pipeline from a YAML file."""
    run_pipeline(spec, workdir)

@app.command()
def list_plugins():
    """List discovered plugin classes."""
    from .registry import list_registered
    for cls in list_registered(): print(cls)

if __name__ == "__main__":
    app()
