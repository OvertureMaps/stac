"""Overture STAC - Generate STAC catalogs for Overture Maps public releases."""

from overture_stac.overture_stac import OvertureRelease
from overture_stac.registry_manifest import RegistryManifest

__version__ = "0.1.0"

__all__ = ["OvertureRelease", "RegistryManifest"]
