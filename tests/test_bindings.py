"""Smoke tests for the Python bindings — surface shape, error mapping, defaults.

Run via `uv run pytest`. `just py-develop` should have installed the module first.
"""

import inspect

import overture_stac
import pytest


def test_expected_symbols_present() -> None:
    assert hasattr(overture_stac, "build_catalog")
    assert hasattr(overture_stac, "OvertureStacError")


def test_error_class_is_runtime_error_subclass() -> None:
    assert issubclass(overture_stac.OvertureStacError, RuntimeError)


def test_build_catalog_has_docstring() -> None:
    doc = overture_stac.build_catalog.__doc__
    assert doc is not None
    assert "release_version" in doc
    assert "schema_version" in doc


def test_schema_version_wrong_type_raises_typeerror() -> None:
    # `schema_version` accepts str or None. Passing anything else raises TypeError
    # at the Python argparse layer, not our custom error.
    with pytest.raises(TypeError):
        overture_stac.build_catalog("2026-07-22.0", 123)  # type: ignore[arg-type,unused-coroutine]


async def test_bogus_data_uri_raises_overturestacerror() -> None:
    # Any URI whose scheme is unknown / bucket is unreachable should surface
    # as OvertureStacError, not a bare exception or SystemError.
    with pytest.raises(overture_stac.OvertureStacError):
        await overture_stac.build_catalog(
            "2026-07-22.0",
            "1.18.0",
            data_uri="s3://this-bucket-should-not-exist-9x8x7",
            extras_uri=None,
            output="/tmp/pytest-overture-stac-bogus",
            debug=True,
        )


async def test_schema_version_none_accepted() -> None:
    # Passing None explicitly should be accepted; the call will fail on the bogus
    # data URI further down, but only AFTER schema_version resolution succeeds.
    with pytest.raises(overture_stac.OvertureStacError):
        await overture_stac.build_catalog(
            "2026-07-22.0",
            None,
            data_uri="s3://this-bucket-should-not-exist-abc",
            extras_uri=None,
            output="/tmp/pytest-overture-stac-schema-none",
            debug=True,
        )


async def test_schema_version_omitted_triggers_http_fetch() -> None:
    # Omitting schema_version triggers auto-infer via HTTP to root_href. Point
    # root_href at an unreachable host so the fetch fails predictably — proves
    # the code path is exercised without needing the real STAC catalog.
    with pytest.raises(overture_stac.OvertureStacError) as excinfo:
        await overture_stac.build_catalog(
            "2026-07-22.0",
            root_href="http://127.0.0.1:1",  # nothing listening
            data_uri="s3://irrelevant",
            extras_uri=None,
            output="/tmp/pytest-overture-stac-autoinfer",
            debug=True,
        )
    # Error should mention the fetch step, not S3
    assert "catalog.json" in str(excinfo.value)


async def test_build_catalog_returns_awaitable() -> None:
    # Sanity: calling build_catalog must produce something awaitable so callers
    # can `await` it. Cancel afterwards to avoid running the actual I/O.
    fut = overture_stac.build_catalog(
        "2026-07-22.0",
        "1.18.0",
        data_uri="s3://nope",
        extras_uri=None,
        output="/tmp/pytest-overture-stac-await",
        debug=True,
    )
    assert inspect.isawaitable(fut)
    if hasattr(fut, "cancel"):
        fut.cancel()
