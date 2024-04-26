"""This module was initially conceived and constructed for use during local development only."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context import get_context
from great_expectations.datasource.fluent import DataAsset
from great_expectations.exceptions import DataContextError

logger = logging.getLogger(__name__)

current_file_path = Path(__file__).resolve()
great_expectations_dir = Path(__file__).parent.parent / Path("gx/")
# Expects this module to be at wm-network/src/<here>
base_project_dir: Path = current_file_path.parent.parent
# Ideally would make below relative for purposes of acting as gx data source,
# but https://github.com/great-expectations/great_expectations/issues/8781
# We will keep it absolute and simply not track 'great_expectations.yml' with git.
# Shouldn't be an issue since we are mainly programmatically modifying it per local instance,
# anyway.
data_dir: Path = base_project_dir

# Configure Great Expectations

context = get_context(context_root_dir=great_expectations_dir)
try:
    datasource = context.sources.add_pandas_filesystem(name=str(data_dir), base_directory=data_dir)
    logger.info(f"Created new gx data source {data_dir}.")
except DataContextError:
    datasource = context.get_datasource(str(data_dir))
    logger.info(f"Fetched existing gx data source {data_dir}.")


# Great Expectations support functions


def build_gx_suite_name(file_path: str) -> str:
    """1:1 relationship between files and suites."""

    return file_path.replace("\\", "-").replace("/", "-")


def add_or_fetch_gx_csv_asset(asset_name: str, batching_regex: str) -> DataAsset:
    try:
        asset = datasource.add_csv_asset(
            name=asset_name,
            batching_regex=batching_regex,  # order_by=["year", "month"],
        )
        logger.info(f"Created new CSV asset {asset_name}.")
    except ValueError:  # Already exists
        asset = datasource.get_asset(asset_name)
        logger.info(f"Fetched existing CSV asset {asset_name}.")

    return asset


def load_gx_validation_spec(csv_path: Path) -> List:
    """
    Looks on the file system alongside the CSV for a Great Expectations spec
    with expectations list at key 'expectations', where each entry is expected to have
    'expectation_type' and other keys that represent kwargs in the GX spec.
    """

    spec_path = csv_path.with_suffix(".yml")
    try:
        with spec_path.open("r") as file:
            content: Dict = yaml.safe_load(file)
    except FileNotFoundError:
        logger.info(f"No associated expectations yml found for {csv_path}.")
        content: Dict = {"expectations": []}

    return content["expectations"]


def is_a_failed_result(result: ExpectationValidationResult) -> bool:
    return result.success is False


# Primary public functions


def validate_csv(
    path: Path, batching_regex: Optional[str] = None
) -> ExpectationSuiteValidationResult:
    """
    Perform validation of file at path using the expectation spec sitting
    next to it, if found.
    """

    # Assumes files are somewhere within this top-level dir
    sub_path = path.resolve().relative_to(base_project_dir)
    batching_regex = batching_regex if batching_regex is not None else re.escape(str(sub_path))
    asset_name = str(sub_path)
    suite_name = build_gx_suite_name(str(sub_path))
    asset = add_or_fetch_gx_csv_asset(asset_name, batching_regex)

    batch_request = asset.build_batch_request()
    context.add_or_update_expectation_suite(suite_name, expectations=load_gx_validation_spec(path))

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    validator.head()
    results: ExpectationSuiteValidationResult = validator.validate()

    # validator.save_expectation_suite()

    return results
