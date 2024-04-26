"""This module was initially conceived and constructed for use during local development only."""

import functools
import logging
import socket
from collections import defaultdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Optional, Dict, Callable, Type, Any
from urllib.parse import urljoin, quote
from uuid import uuid4, UUID
import requests
from requests.exceptions import ConnectionError

import numpy as np
import geopandas
import pandas as pd
import pyogrio
import pyxlsb
from great_expectations.core import ExpectationSuiteValidationResult
from openlineage.client import OpenLineageClient
from openlineage.client.facet import (
    SchemaDatasetFacet,
    SchemaField,
    Assertion,
    DataQualityAssertionsDatasetFacet,
    DataQualityMetricsInputDatasetFacet,
    ColumnMetric,
)
from openlineage.client.run import RunEvent, RunState, Run, Dataset, Job
from pandas.io.parsers import TextFileReader
from pyogrio.errors import DataSourceError

from src.data_quality import validate_csv

OPEN_LINEAGE_HOST = "localhost"
OPEN_LINEAGE_PORT = 5000
OPEN_LINEAGE_JOB_NAMESPACE = "notebook-local"
OPEN_LINEAGE_DATASET_NAMESPACE = "file"
OPEN_LINEAGE_FILE_PATH_FIELD = "name"  # One and the same in this implementation.
CLIENT_SOURCE_CODE_PATH = (
    "https://github.com/OpenLineage/OpenLineage/tree/main/client/python/openlineage/client"
)
open_lineage_base_url = f"http://{OPEN_LINEAGE_HOST}:{OPEN_LINEAGE_PORT}/api/v1/"

logger = logging.getLogger(__name__)

# Temporary container to hold Datasets between time files are read and
# when they are submitted to OpenLineage API. The objective is to register
# all facets at once during RunEvent.START since the marquez UI has not been
# updated to
input_bundles_by_run_id = defaultdict(list)  # { run_id: [Dataset] }
output_bundles_by_run_id = defaultdict(list)  # { run_id: [Dataset] }
failed_datasets_by_run = defaultdict(list)  # { run_id: [Dataset] }
missing_files_by_run = defaultdict(list)  # { run_id: [Dataset.name] }


def _is_tcp_port_open(ip, port) -> bool:
    """Utility for checking whether TCP port is open."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.25)  # Timeout after 0.25s
    result = sock.connect_ex((ip, port))
    sock.close()

    return result == 0  # Returns True if port is open, False otherwise


if _is_tcp_port_open(OPEN_LINEAGE_HOST, OPEN_LINEAGE_PORT) is True:
    client = OpenLineageClient(f"http://{OPEN_LINEAGE_HOST}:{OPEN_LINEAGE_PORT}")
else:
    client = None


# OpenLineage supporting classes/functions


class DatasetTag(Enum):
    FAILED = "FAILED"
    MISSING_FILE = "MISSING_FILE"


def allow_missing_file(
    allowable_exception: Type[Exception], run_id_arg_index=0, path_arg_index=1
) -> Callable:
    """
    Decorator factory to support file ingest functions by allowing the passing of a specific
    Exception raised when a target file is not found during that function's processing. In such instances
    we may still want to perform lineage processing to have the file represented in the pipline.
    This was initially encountered in the context of ad hoc, external (i.e., periodically downloaded
    from elsewhere), itinerant files.

    This functionality does not occur by default. To enable it:
    - Decorate ingest function with this decorator and pass allowable exception.
    - Include 'allow_missing_file=True' in kwargs passed to the ingest function by the caller.

    :param allowable_exception: exception generated/expected if file is not found, anything derived
        from Exception.
    :param run_id_arg_index: position in *args of run ID if not 0
    :param path_arg_index: position in *args of file path if not 1
    """

    def decorator(func) -> Callable:
        """Decorator function to support checking for expected exception."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Optional[Any]:
            """
            Calls function that ingests file, checks for existing of 'allow_missing_file' flag
            and then whether allowable exception is raised.
            """

            allow_missing_file: bool = kwargs.pop("allow_missing_file", False)
            if allow_missing_file is False:
                return func(*args, **kwargs)

            # Accommodate missing input file
            run_id = args[run_id_arg_index]
            try:
                return func(*args, **kwargs)
            except allowable_exception:
                logger.info(
                    f"File not found when {func.__name__} attempted to perform ingest with "
                    f"parameters {args}, {kwargs}. 'allow_missing_file' flag is set such that this"
                    f"is permissible, so bypassing ingest and performing lineage tracking only.... "
                )
                dataset = Dataset(namespace="file", name=str(args[path_arg_index]))
                input_bundles_by_run_id[run_id].append(dataset)
                missing_files_by_run[run_id].append(dataset.name)
                return None

        return wrapper

    return decorator


def allow_missing_service(func):
    """
    Intended to provide backward compatibility with OpenLineage-less use of Jupyter notebooks and
    python scripts, this wrapper function permits and gracefully handles exceptions that arise
    when no OpenLineage service is running.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError:
            logger.info(
                f"The application is attempting to issue a request to the OpenLineage service when that "
                f"service is not found to be running at {OPEN_LINEAGE_HOST}:{OPEN_LINEAGE_PORT}. Did you "
                f"intend to have the service running? Proceeding by bypassing this request...."
            )
            return None

    return wrapper


def build_open_lineage_schema_facet(df: pd.DataFrame) -> SchemaDatasetFacet:
    """Interrogates pandas dataframe and returns OpenLineage schema dataset facet."""

    fields: List[SchemaField] = []
    for col_name, col_type in df.dtypes.to_dict().items():
        fields.append(SchemaField(name=col_name, type=str(col_type)))

    return SchemaDatasetFacet(fields=fields)


def build_open_lineage_assertions_facet(
    results: ExpectationSuiteValidationResult,
) -> DataQualityAssertionsDatasetFacet:
    assertions = [
        Assertion(
            validation_result.expectation_config.expectation_type,
            validation_result.success,
            validation_result.expectation_config.kwargs.get("column"),
        )
        for validation_result in results.results
    ]

    return DataQualityAssertionsDatasetFacet(assertions)


def build_open_lineage_data_quality_facet(df: pd.DataFrame) -> DataQualityMetricsInputDatasetFacet:
    row_count = len(df)
    bytes_ = df.memory_usage(deep=True).sum()
    column_metrics: Dict[str, ColumnMetric] = {}
    for column in df.columns:
        column_metric = ColumnMetric()
        column_data = df[column]
        is_numeric: bool = pd.api.types.is_numeric_dtype(column_data)
        is_boolean: bool = pd.api.types.is_bool_dtype(column_data)

        column_metric.nullCount = column_data.isnull().sum()
        column_metric.distinctCount = column_data.nunique()
        column_metric.count = column_data.count()

        if is_numeric and not is_boolean:
            # Avoid NaN, enable com.fasterxml.jackson `JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS`
            # to allow
            column_metric.sum = None if np.isnan(column_data.sum()) else column_data.sum()
            column_metric.min = None if np.isnan(column_data.min()) else column_data.min()
            column_metric.max = None if np.isnan(column_data.max()) else column_data.max()
            quantiles = column_data.quantile([0.1, 0.25, 0.5, 0.75, 1]).to_dict()
            column_metric.quantiles = {
                k: v if not np.isnan(v) else None for k, v in quantiles.items()
            }

        column_metrics[column_data.name] = column_metric

    logger.debug(f"Column metrics computed: {column_metrics}")

    return DataQualityMetricsInputDatasetFacet(
        rowCount=row_count, bytes=bytes_, columnMetrics=column_metrics
    )


@allow_missing_service
def add_tag_to_dataset(tag: DatasetTag, dataset: Dataset) -> requests.Response:
    """No problem if it's already there."""

    namespace = quote(dataset.namespace, safe="")
    dataset_name = quote(dataset.name, safe="")
    relative_path = f"namespaces/{namespace}/datasets/{dataset_name}/tags/{tag.name}"
    post_url = urljoin(open_lineage_base_url, relative_path)

    return requests.post(post_url, headers={"Accept": "application/json"})


@allow_missing_service
def delete_tag_from_dataset(tag: DatasetTag, dataset: Dataset) -> requests.Response:
    """No problem if it does not exist."""

    namespace = quote(dataset.namespace, safe="")
    dataset_name = quote(dataset.name, safe="")
    relative_path = f"namespaces/{namespace}/datasets/{dataset_name}/tags/{tag.name}"
    delete_url = urljoin(open_lineage_base_url, relative_path)

    return requests.delete(delete_url, headers={"Accept": "application/json"})


def get_dataset_file_path(dataset: Dataset) -> Path:
    """
    This mapping may need to be addressed in a more extensive setup to facilitate
    hand-off/interaction between OpenLineage and Great Expectations (also see OpenLineage
    field physicalName for potential?).
    """

    return getattr(dataset, OPEN_LINEAGE_FILE_PATH_FIELD)


# Primary public functions


def emit_run_event(
    job_name: str,
    run_id: Union[str, UUID],
    event_type: RunState,
    inputs: Optional[List[Dataset]] = None,
    outputs: Optional[List[Dataset]] = None,
) -> None:
    """
    Wrapper function for the OpenLineage client to emit run events. This function can be called
    at any point during a run in order to communicate life-cycle events.
    """

    if client is None:
        logger.info(
            f"The application is attempting to emit an OpenLineage RunEvent when "
            f"no OpenLineage service is found at {OPEN_LINEAGE_HOST}:{OPEN_LINEAGE_PORT}. "
            f"Did you intend to have the service running? Proceeding without OpenLineage support...."
        )
        return

    inputs, outputs = [] if inputs is None else inputs, ([] if outputs is None else outputs)
    client.emit(
        RunEvent(
            producer=CLIENT_SOURCE_CODE_PATH,
            eventType=event_type,
            eventTime=str(datetime.now().isoformat()),
            run=Run(runId=run_id),
            job=Job(namespace=OPEN_LINEAGE_JOB_NAMESPACE, name=job_name),
            inputs=inputs,
            outputs=outputs,
        )
    )


def initialize_run(
    run_id: Optional[str] = None,
    inputs: Optional[List[Dataset]] = None,
    outputs: Optional[List[Dataset]] = None,
) -> str:
    """
    Registers (locally) a new lineage run for a given job.

    :param run_id: Unique identifier (e.g., UUID) for the run. If not provided one will be generated.
    :param inputs: An optional list of OpenLineage Datasets to stage at the start of the run.
    :param outputs: An optional list of OpenLineage Datasets to stage at start of the run. These
        can optionally be updated/appended later in the run.

    :return: A string representing the unique identifier (UUID) generated for the new run.

    Example:
        >>> run_id = initialize_run(inputs=[], outputs=[])
        >>> print(run_id)
        '123e4567-e89b-12d3-a456-426614174000'
    """

    if run_id is None:
        run_id = str(uuid4())

    # Stage any Datasets submitted
    inputs, outputs = inputs if inputs is not None else [], outputs if outputs is not None else []
    input_bundles_by_run_id[run_id] += inputs
    output_bundles_by_run_id[run_id] += outputs

    return run_id


def start_run(
    job_name: str,
    run_id: str,
    additional_inputs: Optional[List[Dataset]] = None,
    additional_outputs: Optional[List[Dataset]] = None,
) -> str:
    """
    Send OpenLineage RunState.START event with arguments passed.

    :param job_name: The name of the job for which the lineage run is being initialized. For
        notebooks, pass the name of the notebook.
    :param run_id: Unique identifier (UUID) for the run.
    :param additional_inputs: An optional list of input OpenLineage Datasets to append to those
        already staged during run init.
    :param additional_outputs: An optional list of output OpenLineage Datasets to append to those
        already staged during run init.
    """

    additional_inputs = [] if additional_inputs is None else additional_inputs
    additional_outputs = [] if additional_outputs is None else additional_outputs
    inputs = input_bundles_by_run_id.get(run_id, []) + additional_inputs
    outputs = output_bundles_by_run_id.get(run_id, []) + additional_outputs

    logger.info(
        f"Starting run {run_id} for job {job_name} with inputs "
        f"{', '.join([i.name for i in inputs])} and outputs "
        f"{', '.join([o.name for o in outputs])}."
    )
    inputs_without_files = [i for i in inputs if i.name in missing_files_by_run[run_id]]
    # Reset all: align with 'no news is good news' (TODO: revisit/formalize this paradigm)
    for dataset in inputs:
        delete_tag_from_dataset(DatasetTag.FAILED, dataset)
        delete_tag_from_dataset(DatasetTag.MISSING_FILE, dataset)

    # Submit run event and datasets with it
    emit_run_event(job_name, run_id, RunState.START, inputs=inputs, outputs=outputs)

    # With datasets registered, inform OpenLineage of some things we know/learned about them
    for dataset in failed_datasets_by_run[run_id]:
        add_tag_to_dataset(DatasetTag.FAILED, dataset)
    for dataset in inputs_without_files:
        add_tag_to_dataset(DatasetTag.MISSING_FILE, dataset)

    return run_id


def complete_run(
    job_name: str, run_id: str, additional_outputs: Optional[List[Dataset]] = None
) -> str:
    """
    Submit run completion event as either RunState.COMPLETE or RunState.FAIL, depending
    whether any dataset validations failed for the run. This means we are treating the validation
    failure of any dataset as a failure of the entire run in OpenLineage.
    """

    additional_outputs = [] if additional_outputs is None else additional_outputs
    outputs = output_bundles_by_run_id.get(run_id, []) + additional_outputs

    failed_datasets = failed_datasets_by_run.get(run_id, [])
    if failed_datasets:
        logger.info(
            f"While completing job {job_name}'s run {run_id} the following datasets were found "
            f"to have failed: {[d.name for d in failed_datasets]}."
        )
        event_type = RunState.FAIL
    else:
        event_type = RunState.COMPLETE
    emit_run_event(job_name, run_id, event_type, outputs=outputs)

    return run_id


@allow_missing_file(FileNotFoundError)
def read_csv(run_id: str, path: Union[str, Path], **pandas_kwargs) -> pd.DataFrame:
    """
    Wrapper for native pandas method. Also performs lineage and data quality tasks
    for the CSV, adding metadata facets to the dataset and adding to input bundle to
    be submitted at run start.
    """

    if isinstance(path, str):
        path = Path(path)

    # Redundant (2) reads for now
    df = pd.read_csv(path, **pandas_kwargs)
    results: ExpectationSuiteValidationResult = validate_csv(Path(path))

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    dataset = Dataset(
        namespace="file",
        name=str(path),
        facets={
            "schema": build_open_lineage_schema_facet(df),
            # To inputFacets when marquez web supports
            "dataQualityMetrics_": build_open_lineage_data_quality_facet(df),
            # To inputFacets when marquez web supports
            "dataQualityAssertions_": build_open_lineage_assertions_facet(results),
        },
    )
    input_bundles_by_run_id[run_id].append(dataset)  # Queue for run start event
    if results.success is False:  # Track locally for use during run completion/logging
        failed_datasets_by_run[run_id].append(dataset)

    return df


@allow_missing_file(FileNotFoundError)
def read_table(
    run_id: str, path: Union[str, Path], **pandas_kwargs
) -> pd.DataFrame | TextFileReader:
    """
    Wrapper for native pandas method, performing lineage tasks for each table and adding
    it to input bundle to be submitted at run init.
    """

    if isinstance(path, str):
        path = Path(path)

    df = pd.read_table(path, **pandas_kwargs)

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    input_bundles_by_run_id[run_id].append(
        Dataset(
            namespace="file",
            name=str(path),
            facets={
                "schema": build_open_lineage_schema_facet(df),
                # To inputFacets when marquez web supports
                "dataQualityMetrics_": build_open_lineage_data_quality_facet(df),
            },
        )
    )

    return df


@allow_missing_file(FileNotFoundError)
def read_excel(run_id: str, path: Union[str, Path], **pandas_kwargs) -> pd.DataFrame:
    """
    Wrapper for native pandas method, performing lineage tasks for each table and adding
    it to input bundle to be submitted at run init.
    """

    if isinstance(path, str):
        path = Path(path)

    df = pd.read_excel(path, **pandas_kwargs)

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    input_bundles_by_run_id[run_id].append(
        Dataset(
            namespace="file",
            name=str(path),
            facets={
                "schema": build_open_lineage_schema_facet(df),
                # To inputFacets when marquez web supports
                "dataQualityMetrics_": build_open_lineage_data_quality_facet(df),
            },
        )
    )

    return df


@allow_missing_file(pyogrio.errors.DataSourceError)
def read_dataframe(run_id: str, path: Union[str, Path], **pyogrio_kwargs) -> geopandas.GeoDataFrame:
    """
    Wrapper for native pyogrio method for reading spatial data layer GeoDataFrame. Performs
    lineage tasks and adds it to input bundle to be submitted at run init.
    """

    if isinstance(path, str):
        path = Path(path)

    gdf = pyogrio.read_dataframe(path, **pyogrio_kwargs)

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    input_bundles_by_run_id[run_id].append(
        Dataset(
            namespace="file",
            name=str(path),
            facets={
                "schema": build_open_lineage_schema_facet(gdf),
                # To inputFacets when marquez web supports
                "dataQualityMetrics_": build_open_lineage_data_quality_facet(gdf),
            },
        )
    )

    return gdf


def open_workbook(run_id: str, path: Union[str, Path]) -> pyxlsb.Workbook:
    """
    Wrapper for native pyxlsb method for reading an Excel workbook. Performs
    lineage tasks and adds it to input bundle to be submitted at run init.
    Responsibility for closing the returned Workbook object falls on the caller.
    """

    path = str(path)

    wb = pyxlsb.open_workbook(path)

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    input_bundles_by_run_id[run_id].append(
        Dataset(
            namespace="file",
            name=str(path),
            # Multiple sheets per workbook, no schema/facets for now
            facets={},
        )
    )

    return wb


def to_csv(run_id: str, path: Union[str, Path], df: Optional[pd.DataFrame], **pandas_kwargs) -> str:
    """
    Wrapper for native pandas method, performing lineage tasks for each CSV and adding
    it to output bundle to be submitted at run completion.
    """

    if df is None:  # No data but let's account for lineage
        facets = {}
    else:
        df.to_csv(path, **pandas_kwargs)
        facets = {
            "schema": build_open_lineage_schema_facet(df),
            # To inputFacets once supported marquez
            "dataQualityMetrics_": build_open_lineage_data_quality_facet(df),
        }

    # https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets
    output_bundles_by_run_id[run_id].append(
        Dataset(namespace="file", name=str(path), facets=facets)
    )

    return run_id
