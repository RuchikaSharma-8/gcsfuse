from google.cloud import bigquery

def write_fio_metrics_to_bigquery(gcsfuse_flags, branch, end_date, values_all_jobs):

  # Instantiate a BigQuery client
  client = bigquery.Client()

  query_create_table_experiment_configuration = """
      CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.experiment_configuration(
        configuration_id INT64, 
        gcsfuse_flags STRING, 
        branch STRING, 
        end_date TIMESTAMP,  
        PRIMARY KEY (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing Job Configurations and respective VM instance name on which the job was run');
  """
  query_create_table_fio_metrics = """
      CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.fio_metrics(
        configuration_id INT64, 
        test_type STRING, 
        num_threads INT64, 
        file_size_kb INT64, 
        start_time INT64, 
        end_time INT64, 
        iops FLOAT64, 
        bandwidth_bytes_per_sec INT64, 
        IO_bytes INT64, 
        min_latency FLOAT64, 
        max_latency FLOAT64, 
        mean_latency FLOAT64, 
        percentile_latency_20 FLOAT64, 
        percentile_latency_50 FLOAT64, 
        percentile_latency_90 FLOAT64, 
        percentile_latency_95 FLOAT64, 
        FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing FIO metrics extracted from periodic performance load testing');
  """
  query_create_table_vm_metrics = """
      CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.vm_metrics(
        configuration_id INT64, 
        end_time INT64, 
        cpu_utilization_peak_percentage FLOAT64, 
        cpu_utilization_mean_percentage FLOAT64, 
        received_bytes_peak_bytes_per_sec FLOAT64, 
        received_bytes_mean_bytes_per_sec FLOAT64, 
        read_bytes_count INT64,
        ops_error_count INT64, 
        ops_mean_latency_sec FLOAT64, 
        throughput FLOAT64, 
        memory_utilization FLOAT64, 
        iops FLOAT64, 
        ops_count_list_object INT64, 
        ops_count_create_object INT64, 
        ops_count_stat_object INT64, 
        ops_count_new_reader INT64, 
        FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing VM metrics extracted from periodic performance load testing');
  """
  query_create_table_ls_metrics = """
      CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.ls_metrics(
        configuration_id INT64,
        test_type STRING, 
        num_files INT64, 
        num_samples INT64, 
        min_latency_msec FLOAT64,
        max_latency_msec FLOAT64,
        mean_latency_msec FLOAT64, 
        median_latency_msec FLOAT64, 
        standard_dev_msec FLOAT64, 
        percentile_latency_0 FLOAT64, 
        percentile_latency_20 FLOAT64, 
        percentile_latency_50 FLOAT64, 
        percentile_latency_90 FLOAT64, 
        percentile_latency_95 FLOAT64, 
        percentile_latency_100 FLOAT64, 
        cpu_utilization_peak_percentage FLOAT64, 
        cpu_utilization_mean_percentage FLOAT64,
        memory_utilization_ram FLOAT64, 
        FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing GCSFUSE metrics extracted from listing benchmark tests');
  """

  results = client.query(query_create_table_experiment_configuration)
  print(results)
  results = client.query(query_create_table_fio_metrics)
  print(results)
  results = client.query(query_create_table_vm_metrics)
  print(results)
  results = client.query(query_create_table_ls_metrics)
  print(results)

  query_insert_configuration = """
      INSERT INTO gcsfuse-intern-project-2023.performance_metrics.experiment_configuration 
      VALUES (1, '--implicit-dirs --max-conns-per-host 100 --enable-storage-client-library --debug_fuse --debug_gcs --log-file gcsfuse-logs.txt --log-format "text" --stackdriver-export-interval=30s', 
          'master', '2023-12-25 05:30:00+00');
  """
  results = client.query(query_insert_configuration)
  print(results)

  print(gcsfuse_flags)
  print(branch)
  print(end_date)

  query = ("SELECT configuration_id FROM `{}.{}.{}` WHERE gcsfuse_flags='{}' AND branch='{}' AND end_date='{}'"
           .format('gcsfuse-intern-project-2023', 'performance_metrics', 'fio_metrics', gcsfuse_flags, branch, end_date))

  try:
    query_job = client.query(query)
    is_exist = len(list(query_job.result())) >= 1
    print('Exist id' if is_exist else 'Not exist id')
    return is_exist
  except Exception as e:
    print("Error")
    print(e)

  dataset_ref = client.dataset('performance_metrics')

  table_ref = dataset_ref.table('fio_metrics')
  table = client.get_table(table_ref)  # API call

  for values in values_all_jobs:
    print(values)
    rows_to_insert = [
        (1, values[0], values[1], values[2], values[3], values[4], values[5],  values[6], values[7], values[8], values[9], values[10], values[11], values[12], values[13], values[14])
    ]
    errors = client.insert_rows(table, rows_to_insert)
    print(errors)

  # query_get_configuration_id = """
  #     SELECT configuration_id
  #     FROM gcsfuse-intern-project-2023.performance_metrics.experiment_configuration
  #     WHERE gcsfuse_flags = '{}'
  #     AND branch = '{}'
  #     AND end_date = '{}'
  # """.format(gcsfuse_flags, branch, end_date)
  # results = client.query(query_get_configuration_id)
  # config_id = 0
  # for row in results:
  #   config_id = row['configuration_id']
  #   print(config_id)
  #
  # print(config_id)


def write_vm_metrics_to_bigquery(gcsfuse_flags, branch, end_date, values_all_jobs):

  # Instantiate a BigQuery client
  client = bigquery.Client()

  print(gcsfuse_flags)
  print(branch)
  print(end_date)

  # query_get_configuration_id = """
  #     SELECT configuration_id
  #     FROM gcsfuse-intern-project-2023.performance_metrics.experiment_configuration
  #     WHERE gcsfuse_flags = '{}'
  #     AND branch = '{}'
  #     AND end_date = '{}'
  # """.format(gcsfuse_flags, branch, end_date)
  # results = client.query(query_get_configuration_id)
  # config_id = 0
  # for row in results:
  #   config_id = row['configuration_id']
  #   print(config_id)
  #
  # print(config_id)

  dataset_ref = client.dataset('performance_metrics')

  table_ref = dataset_ref.table('vm_metrics')
  table = client.get_table(table_ref)  # API call

  for values in values_all_jobs:
    print(values)
    rows_to_insert = [
        (1, values[1], values[2], values[3], values[4], values[5])
    ]
    errors = client.insert_rows(table, rows_to_insert)
    print(errors)


def write_ls_metrics_to_bigquery(gcsfuse_flags, branch, end_date, gcsfuse_values, pd_values, gcsfuse_values_vm, pd_values_vm):

  # Instantiate a BigQuery client
  client = bigquery.Client()

  query_create_table_ls_metrics = """
      CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.ls_metrics(
        configuration_id INT64,
        test_type STRING, 
        num_files INT64, 
        num_samples INT64, 
        min_latency_msec FLOAT64,
        max_latency_msec FLOAT64,
        mean_latency_msec FLOAT64, 
        median_latency_msec FLOAT64, 
        standard_dev_msec FLOAT64, 
        percentile_latency_0 FLOAT64, 
        percentile_latency_20 FLOAT64, 
        percentile_latency_50 FLOAT64, 
        percentile_latency_90 FLOAT64, 
        percentile_latency_95 FLOAT64, 
        percentile_latency_100 FLOAT64, 
        cpu_utilization_peak_percentage FLOAT64, 
        cpu_utilization_mean_percentage FLOAT64,
        memory_utilization_ram FLOAT64, 
        FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
      ) OPTIONS (description = 'Table for storing GCSFUSE metrics extracted from listing benchmark tests');
  """

  results = client.query(query_create_table_ls_metrics)
  print(results)

  print(gcsfuse_flags)
  print(branch)
  print(end_date)

  dataset_ref = client.dataset('performance_metrics')

  table_ref = dataset_ref.table('ls_metrics')

  table = client.get_table(table_ref)  # API call

  for row1, row2 in zip(gcsfuse_values, gcsfuse_values_vm):
    print(row1)
    print(row2)
    rows_to_insert = [
        (1, 'gcsfuse', row1[2], row1[4], row1[8], row1[17], row1[5], row1[6], row1[7], row1[9], row1[10], row1[11], row1[12], row2[0], row2[1], ''),
    ]
    errors = client.insert_rows(table, rows_to_insert)
    print(errors)

  for row1, row2 in zip(pd_values, pd_values_vm):
    print(row1)
    print(row2)
    rows_to_insert = [
        (1, 'disk', row1[2], row1[4], row1[8], row1[17], row1[5], row1[6], row1[7], row1[9], row1[10], row1[11], row1[12], row2[0], row2[1], ''),
    ]
    errors = client.insert_rows(table, rows_to_insert)
    print(errors)


