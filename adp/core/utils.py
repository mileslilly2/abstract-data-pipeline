# adp/core/utils.py
"""Utility helpers: logging, simple retry decorator, class resolver."""

from __future__ import annotations
import logging
import time
from importlib import import_module
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def make_logger(name: str = "adp", level: int = logging.INFO) -> logging.Logger:
    """Create a simple logger if none exists for `name`."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)
        logger.setLevel(level)
    return logger


def resolve_class(path: str) -> type:
    """Resolve a class or callable from a string path.

    Accepts:
      - 'module.submodule:ClassName'  (colon form)
      - 'module.submodule.ClassName'  (dotted form)

    Raises ModuleNotFoundError / AttributeError on failure.
    """
    if ":" in path:
        mod_path, cls_name = path.split(":", 1)
    else:
        parts = path.split(".")
        mod_path, cls_name = ".".join(parts[:-1]), parts[-1]

    module = import_module(mod_path)
    return getattr(module, cls_name)


def retry(
    tries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    allowed_exceptions: tuple[type, ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """A small retry decorator (no external dependency).

    Example:
        @retry(tries=5, delay=0.5)
        def call_api(...):
            ...
    """
    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        def wrapped(*args, **kwargs) -> T:
            _tries, _delay = tries, delay
            last_exc: Optional[BaseException] = None
            while _tries > 0:
                try:
                    return fn(*args, **kwargs)
                except allowed_exceptions as e:
                    last_exc = e
                    _tries -= 1
                    if _tries <= 0:
                        break
                    time.sleep(_delay)
                    _delay *= backoff
            # re-raise the last exception
            raise last_exc  # type: ignore
        wrapped.__name__ = fn.__name__
        wrapped.__doc__ = fn.__doc__
        return wrapped
    return deco
