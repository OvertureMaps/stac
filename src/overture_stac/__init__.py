"""Overture STAC - Generate STAC catalogs for Overture Maps public releases."""

from importlib.metadata import version

from overture_stac.overture_stac import OvertureRelease
from overture_stac.registry_manifest import RegistryManifest

__version__ = version("overture-stac")

__all__ = ["OvertureRelease", "RegistryManifest"]
