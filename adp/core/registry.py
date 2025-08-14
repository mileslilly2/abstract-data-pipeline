import importlib, pkg_resources

def list_registered():
    for ep in pkg_resources.iter_entry_points(group="adp.plugins"):
        yield ep.module_name + ":" + ep.name

def resolve_class(path: str):
    # supports full path "package.module:Class"
    if ":" in path:
        mod, name = path.split(":")
    else:
        # also accept dotted path "package.module.Class"
        parts = path.split("."); mod, name = ".".join(parts[:-1]), parts[-1]
    m = importlib.import_module(mod)
    return getattr(m, name)
