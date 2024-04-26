# data_lineage and data_quality

Intended for in-place instrumentation of Jupyter notebooks and other Python scripts.

## Key Points
- CSV/pandas is the only input/output supported by instrumentation: `read_csv` and `to_csv` replace pandas equivalents, accept the same + additional args, and perform same function.
- OpenLineage has the notion of _job_, which should generally correspond 1:1 with a Jupyter notebook. Multiple runs will occur underneath each job (e.g., each time you run the notebook).
- OpenLineage supports storage of standard and ad hoc sets of metadata via _facets_. These can be found in the Marquez UI upon drilling into a dataset (or job).
- OpenLineage uses the notion of _namespaces_. By default, for a given project there will be two namespaces: 'file' for all datasets, and 'notebook-local' for jobs.

## Dependencies

- Dockerized services represented in `dev-utils/docker-compose.yml`: `docker-compose up`. This will...
  - Start and seed a postgres database (stores OpenLineage events)
  - Start the OpenLineage (marquez) API service (port 5000)
  - Start the Marquez UI (http://localhost:3000)
- With virtual env active, one-time execution of: `great_expectations init`

## Instrumentation

### Notebook: Input and Output Lineage

```python
# At or near top of notebook
import pandas as pd
from src.data_lineage import initialize_run, start_run, complete_run, read_csv, to_csv

job_name = 'your_notebook'  # Can be whatever you want, but notebook name is good
run_id = initialize_run()  # Initialize run and receive run ID
# Load CSVs: address *all* CSV inputs, here
input_one: pd.DataFrame = read_csv('relative/input/path/one', run_id)
input_two: pd.DataFrame = read_csv('relative/input/path/two', run_id)
start_run(job_name, run_id)

# ...

# Along the way
to_csv('relative/output/path/one', run_id)
to_csv('relative/output/path/two', run_id)

# ...

# At or near bottom of notebook
complete_run(job_name, run_id)
```

### File: Input Validation

Next to each CSV being consumed as an input, add a file with the same name except with a '.yml'
extension instead of '.csv'. Include it in a Great Expectations specification to be used for validation,
results from which will be appended to OpenLineage data and viewable in the Marquez UI.

Currently, if any dataset fails validation:
- The run is marked as failed (and the job will show up in the Marquez UI as failed).
- The dataset is tagged with 'FAILED' (viewable in Marquez UI).
- The outcome of specific expectations can be viewed in the Marquez UI by drilling into the dataset's assertions facet.

An example specification (`3p_hauling_sites2.yml`):

```yaml
expectations:
  - expectation_type: expect_column_values_to_be_in_set
    kwargs:
      column: ownership
      value_set: ["3P", "OtherTypes..."]
      mostly: 0.99  # allowing for some missing or incorrect values

  - expectation_type: expect_column_values_to_be_of_type
    kwargs:
      column: active
      type_: bool

  - expectation_type: expect_column_values_to_be_in_set
    kwargs:
      column: active
      value_set: [true]  # Pandas will take it from here, re: boolean

  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: tons_per_time_msw
      min_value: 0
      max_value: 250000  # Assuming no upper limit

  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: latitude
      min_value: -90
      max_value: 90

  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: longitude
      min_value: -180
      max_value: 180

  - expectation_type: expect_column_values_to_match_regex
    kwargs:
      column: code
      regex: "^HS-[A-Z]{2}-[A-Za-z\\.]+$"
```

### Customizing

OpenLineage events can be generated fairly easy at any time during a run, in order to contribute additional metadata related to the job or datasets. Examples to come...

