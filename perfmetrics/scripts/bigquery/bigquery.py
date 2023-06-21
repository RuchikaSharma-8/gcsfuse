# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Python module for setting up tables in BigQuery.

This python module creates the dataset and the tables that will store experiment
configurations and metrics data in BigQuery. It can also be used to upload data to the tables.

Note:
  Make sure BigQuery API is enabled for the project
"""
import sys
import uuid
import time
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob

class ExperimentsGCSFuse:
  """
    This class can used to create the dataset that will store the tables and the tables
    to store the experiment configurations and the tables which will store the metrics data.
    It also provides methods to upload data to the tables.

    Attributes:
      project_id (str): The project on pantheon in which dataset and tables will be created
      dataset_id (str): The name of the dataset in the project that will store the tables
      bq_client (Optional[google.cloud.bigquery.client.Client]): The client for interacting with Bigquery
  """

  CONFIGURATION_TABLE_ID = 'experiment_configuration'
  FIO_TABLE_ID = 'read_write_fio_metrics'
  VM_TABLE_ID = 'read_write_vm_metrics'
  LS_TABLE_ID = 'list_metrics'

  def __init__(self, project_id, dataset_id, bq_client=None):
    """
      Initializes a new instance of ExperimentsGCSFuse.

      Args:
        project_id (str): The project on pantheon in which dataset and tables will be created
        dataset_id (str): The name of the dataset in the project that will store the tables
        bq_client (Optional[google.cloud.bigquery.client.Client]): The client for interacting with Bigquery
    """
    if bq_client is None:
      self.client = bigquery.Client(project=project_id)
    self.project_id = project_id
    self.dataset_id = dataset_id

  @property
  def dataset_ref(self):
    """
      Gets the reference of the dataset

      Returns:
        google.cloud.bigquery.dataset.Dataset: The retrieved dataset object
    """
    return self.client.get_dataset(self.dataset_id)

  def _execute_query_and_check_for_error(self, query) -> QueryJob:
    """Executes the query in BigQuery.

    Args:
      query (str): Query that will be executed in BigQuery.

    Raises:
      Aborts the program if error is encountered while executing th query.
    """
    job = self.client.query(query)
    # Wait for query to be completed
    job.result()
    if job.errors:
      for error in job.errors:
        print(f"Error message: {error['message']}")
      sys.exit(1)
    return job


  def _check_if_config_valid(self, exp_config_id) -> bool:
    """Checks if exp_config_id exists in the experiment_configuration table.

    Args:
      exp_config_id (str): An id that uniquely identifies an experiment

    Returns:
      bool: Returns true if exp_config_id exists, false otherwise.
    """
    query_check_if_config_valid = """
      SELECT *
      FROM `{}.{}.{}`
      WHERE configuration_id = '{}'
    """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID, exp_config_id)

    job = self._execute_query_and_check_for_error(query_check_if_config_valid)
    row_count = job.result().total_rows
    if row_count:
      return True
    return False

  def setup_dataset_and_tables(self):

    """Creates the dataset to store the tables and the experiment configuration table to store the configuration details and
       creates the list_metrics, read_write_fio_metrics and read_write_vm_metrics tables
       to store the metrics data if they don't already exist in the dataset
    """
    # Create dataset if not exists
    dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
    self.client.create_dataset(dataset, exists_ok=True)
    # Wait for the dataset to be created and ready to be referenced
    time.sleep(120)

    # Query for creating experiment_configuration table if it does not exist
    query_create_table_experiment_configuration = """
      CREATE TABLE IF NOT EXISTS {}.{}.{}(
        configuration_id STRING,
        configuration_name STRING,
        gcsfuse_flags STRING,
        branch STRING,
        end_date TIMESTAMP,
        PRIMARY KEY (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing Job Configurations and respective VM instance name on which the job was run');
    """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID)

    # Query for creating fio_metrics table
    query_create_table_fio_metrics = """
      CREATE TABLE IF NOT EXISTS {}.{}.{}(
        configuration_id STRING, 
        start_time_build TIMESTAMP,
        test_type STRING, 
        num_threads INT64, 
        file_size_kb INT64, 
        block_size_kb INT64,
        start_time INT64, 
        end_time INT64, 
        iops FLOAT64, 
        bandwidth_bytes_per_sec INT64, 
        io_bytes INT64, 
        min_latency FLOAT64, 
        max_latency FLOAT64, 
        mean_latency FLOAT64, 
        percentile_latency_20 FLOAT64, 
        percentile_latency_50 FLOAT64, 
        percentile_latency_90 FLOAT64, 
        percentile_latency_95 FLOAT64, 
        FOREIGN KEY(configuration_id) REFERENCES {}.{} (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing FIO metrics extracted from experiments.');
    """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.FIO_TABLE_ID, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID)

    # Query for creating vm_metrics table
    query_create_table_vm_metrics = """
      CREATE TABLE IF NOT EXISTS {}.{}.{}(
        configuration_id STRING, 
        start_time_build TIMESTAMP,
        end_time INT64, 
        cpu_utilization_peak_percentage FLOAT64, 
        cpu_utilization_mean_percentage FLOAT64, 
        received_bytes_peak_per_sec FLOAT64, 
        received_bytes_mean_per_sec FLOAT64, 
        read_bytes_count INT64,
        ops_error_count INT64, 
        ops_mean_latency_sec FLOAT64, 
        sent_bytes_peak_per_sec FLOAT64, 
        sent_bytes_mean_per_sec FLOAT64, 
        sent_bytes_count INT64,
        iops FLOAT64, 
        ops_count_list_object INT64, 
        ops_count_create_object INT64, 
        ops_count_stat_object INT64, 
        ops_count_new_reader INT64, 
        FOREIGN KEY(configuration_id) REFERENCES {}.{} (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing VM metrics extracted from experiments.');
    """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.VM_TABLE_ID, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID)

    # Query for creating ls_metrics table
    query_create_table_ls_metrics = """
      CREATE TABLE IF NOT EXISTS {}.{}.{}(
        configuration_id STRING,
        start_time_build TIMESTAMP,
        mount_type STRING, 
        command STRING,
        start_time FLOAT64, 
        end_time FLOAT64,
        num_files INT64, 
        num_samples INT64, 
        min_latency_msec FLOAT64,
        max_latency_msec FLOAT64,
        mean_latency_msec FLOAT64, 
        median_latency_msec FLOAT64, 
        standard_dev_latency_msec FLOAT64, 
        percentile_latency_20 FLOAT64, 
        percentile_latency_50 FLOAT64, 
        percentile_latency_90 FLOAT64, 
        percentile_latency_95 FLOAT64, 
        cpu_utilization_peak_percentage FLOAT64, 
        cpu_utilization_mean_percentage FLOAT64,            
        received_bytes_peak_per_sec FLOAT64, 
        received_bytes_mean_per_sec FLOAT64,
        FOREIGN KEY(configuration_id) REFERENCES {}.{} (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing GCSFUSE metrics extracted from list experiments.');
    """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.LS_TABLE_ID, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID)

    self._execute_query_and_check_for_error(query_create_table_experiment_configuration)
    self._execute_query_and_check_for_error(query_create_table_fio_metrics)
    self._execute_query_and_check_for_error(query_create_table_vm_metrics)
    self._execute_query_and_check_for_error(query_create_table_ls_metrics)

  def get_experiment_configuration_id(self, gcsfuse_flags, branch, end_date, config_name) -> str:

    """Gets the configuration ID of the experiment from experiment details
       If experiment configuration exists: Check if end date needs update and
                                           then return the configuration ID
       Else: Insert new experiment configuration and return the configuration ID

    Args:
      gcsfuse_flags (str): Set of flags the gcsfuse flags used for experiment.
      branch (str): GCSFuse repo branch used for building GCSFuse.
      end_date (timestamp): Date till when experiments of this configuration are run.
      config_name (str): Name of the experiment configuration.

    Returns:
      str: Configuration ID of the experiment
    """
    # Check if the experiment configuration is already present in table
    query_check_config_exists = """
      SELECT configuration_id
      FROM `{}.{}.{}`
      WHERE gcsfuse_flags = '{}'
      AND branch = '{}'
      AND configuration_name = '{}'
#     """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID, gcsfuse_flags, branch, config_name)

    job = self._execute_query_and_check_for_error(query_check_config_exists)
    result_count = job.result().total_rows

    # If result empty, then experiment configuration not present -> insert new experiment configuration -> return configuration ID
    if result_count == 0:
      ds_ref = self.dataset_ref
      table_ref = ds_ref.table(ExperimentsGCSFuse.CONFIGURATION_TABLE_ID)
      table = self.client.get_table(table_ref)
      uuid_str = str(uuid.uuid4())
      rows_to_insert = [(uuid_str, config_name, gcsfuse_flags, branch, end_date)]
      result = self.client.insert_rows(table, rows_to_insert)
      if result:
        print(f'Error inserting experiment configuration: {result}')
        sys.exit(1)
      return uuid_str

    # If more than 1 result -> duplicate experiment configuration present -> throw error
    elif result_count > 1:
      print("Duplicate experiment configurations exist. Data corrupted")
      sys.exit(1)

    # If exactly one result -> update end date -> return configuration ID
    else:
      config_id = None
      for row in job:
        config_id = row['configuration_id']
      query_update_end_date = """
        UPDATE `{}.{}.{}`
        SET end_date = '{}'
        WHERE configuration_id = '{}'
        """.format(self.project_id, self.dataset_id, ExperimentsGCSFuse.CONFIGURATION_TABLE_ID, end_date, config_id)
      self._execute_query_and_check_for_error(query_update_end_date)
      return config_id

  def upload_metrics_to_table(self, table_name, config_id, start_time_build, metrics_data):

    """Uploads metrics_data to the table corresponding to 'table_name'.

    Args:
      table_name (str): Table name corresponding to the table id to which results are being uploaded
      config_id (str): config_id of the experiment for which results are being uploaded
      start_time_build (timestamp): Start time of the build
      metrics_data (list): A 2D list containing the experiment results
    """

    # Check if the configuration ID of the experiment is valid
    config_valid = self._check_if_config_valid(config_id)

    if not config_valid:
      print("Invalid configuration ID")
      sys.exit(1)
    else:
      # Get the dataset reference
      ds_ref = self.dataset_ref
      table_id = None
      # Get the table ID based on the table name
      if table_name == 'fio':
        table_id = ExperimentsGCSFuse.FIO_TABLE_ID
      elif table_name == 'vm':
        table_id = ExperimentsGCSFuse.VM_TABLE_ID
      elif table_name == 'list':
        table_id = ExperimentsGCSFuse.LS_TABLE_ID
      else:
        print("Wrong table name passed to bigquery module: ", table_name)
        sys.exit(1)
      # Get the table reference from the ID
      table_ref = ds_ref.table(table_id)
      table = self.client.get_table(table_ref)

      for row in metrics_data:
        row_to_insert = [
            (config_id, start_time_build) + tuple(row)
        ]
        result = self.client.insert_rows(table, row_to_insert)
        if result:
          print(f'Error inserting data: {result}')
          sys.exit(1)
