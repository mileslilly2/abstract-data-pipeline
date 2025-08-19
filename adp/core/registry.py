# adp/registry.py
"""Plugin registry helpers.

This module helps discover plugins registered under the `adp.plugins` entry
point group. Plugins should register in their package's pyproject.toml / setup.cfg
with an entry point in the `adp.plugins` group.

Example entry in pyproject.toml:
[project.entry-points."adp.plugins"]
weather_gov = "adp_plugins.disaster.weather_gov:WeatherGovAlertsSource"
"""

from __future__ import annotations
import sys
from typing import List, Tuple, Optional

# Prefer importlib.metadata (py3.8+). Fall back to pkg_resources if needed.
try:
    # Python 3.10+: entry_points() returns Selection-like object; filter accordingly.
    from importlib.metadata import entry_points, EntryPoint  # type: ignore
    _HAS_IM = True
except Exception:
    _HAS_IM = False
    try:
        import pkg_resources  # type: ignore
    except Exception:
        pkg_resources = None  # type: ignore


ENTRY_GROUP = "adp.plugins"


def list_registered_plugins() -> List[Tuple[str, str]]:
    """Return a list of (name, value) for discovered entry points.

    The `value` is the entry point string (e.g. 'module.sub:Class').
    """
    results: List[Tuple[str, str]] = []
    if _HAS_IM:
        eps = entry_points()
        # importlib.metadata.entry_points returns different types across versions
        if isinstance(eps, dict):
            group_eps = eps.get(ENTRY_GROUP, [])
        else:
            # newer API: filter selection
            group_eps = [ep for ep in eps if getattr(ep, "group", None) == ENTRY_GROUP]
        for ep in group_eps:
            # ep.name, ep.value or f"{ep.module}:{ep.attr}" depending on object
            name = getattr(ep, "name", str(ep))
            val = getattr(ep, "value", None)
            if val is None:
                # try to reconstruct
                module = getattr(ep, "module", None)
                attr = getattr(ep, "attr", None)
                if module and attr:
                    val = f"{module}:{attr}"
                else:
                    val = str(ep)
            results.append((name, val))
        return results

    # fallback to pkg_resources
    if pkg_resources is not None:
        for ep in pkg_resources.iter_entry_points(group=ENTRY_GROUP):
            results.append((ep.name, ep.module_name + ":" + ep.attrs[0] if ep.attrs else ep.module_name))
        return results

    # no mechanism available
    return results


def load_plugin_class(name: str):
    """Load the class pointed to by the entry point name.

    Returns the resolved object (usually a class) or raises LookupError.
    """
    if _HAS_IM:
        eps = entry_points()
        if isinstance(eps, dict):
            group_eps = eps.get(ENTRY_GROUP, [])
        else:
            group_eps = [ep for ep in eps if getattr(ep, "group", None) == ENTRY_GROUP]
        for ep in group_eps:
            if getattr(ep, "name", None) == name:
                # .load() will import and return the referenced object
                return ep.load()
        raise LookupError(f"No adp.plugins entry point named {name!r}")
    # pkg_resources fallback
    if pkg_resources is not None:
        ep = pkg_resources.iter_entry_points(group=ENTRY_GROUP)
        for e in ep:
            if e.name == name:
                return e.load()
        raise LookupError(f"No adp.plugins entry point named {name!r}")

    raise LookupError("No plugin discovery mechanism available (importlib.metadata or pkg_resources not found).")


def resolve_entry_point_value(name: str) -> Optional[str]:
    """Return the raw entry point value string for `name`, if present."""
    for nm, val in list_registered_plugins():
        if nm == name:
            return val
    return None
