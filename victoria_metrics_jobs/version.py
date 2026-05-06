from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def get_app_version(dist_name: str = "victoria_metrics_jobs") -> str:
    """
    Return the installed package version for this service.

    If the package isn't installed in the current environment (e.g. running from a
    source checkout without installation), returns "unknown".
    """
    try:
        return version(dist_name)
    except PackageNotFoundError:
        return "unknown"

