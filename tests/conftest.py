"""Conftest module."""

import platform

from rdw import PROJECT_DIR
from tests.fixtures.spark_fixture import spark_session

CATALOG_DIR = PROJECT_DIR / "tests" / "catalog"
CATALOG_DIR.mkdir(parents=True, exist_ok=True)  # noqa

