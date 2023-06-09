from google.cloud import bigquery

# Instantiate a BigQuery client
client = bigquery.Client()

id = 0

def get_id():
  id = id + 1
  return id

query_create_table_experiment_configuration = """
    CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.experiment_configuration(
      configuration_id INT64, 
      gcsfuse_flags STRING, 
      branch STRING, 
      end_date TIMESTAMP, 
      vm_instance STRING, 
      PRIMARY KEY (gcsfuse_flags, branch, end_date) NOT ENFORCED
    ) OPTIONS (description = 'Table for storing Job Configurations and respective VM    instance name on which the job was run');
"""
query_create_table_fio_metrics = """
    CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.fio_metrics(
      configuration_id INT64, 
      read_or_write STRING, 
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
      gcsfuse_flags STRING, 
      branch STRING, 
      end_time_vm INT64, 
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
query_create_table_ls_metrics_gcsfuse = """
    CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.ls_metrics_gcsfuse(
      configuration_id INT64, 
      gcsfuse_flags STRING, 
      branch STRING, 
      test_description STRING, 
      command STRING, 
      num_files INT64, 
      num_folder INT64, 
      num_samples INT64, 
      mean_latency_msec FLOAT64, 
      meadian_latency_msec FLOAT64, 
      standard_dev_msec FLOAT64, 
      percentile_latency_0 FLOAT64, 
      percentile_latency_20 FLOAT64, 
      percentile_latency_50 FLOAT64, 
      percentile_latency_90 FLOAT64, 
      percentile_latency_95 FLOAT64, 
      percentile_latency_98 FLOAT64, 
      percentile_latency_99 FLOAT64, 
      percentile_latency_99pt5 FLOAT64, 
      percentile_latency_99pt9 FLOAT64, 
      percentile_latency_100 FLOAT64, 
      cpu_utilization_peak_percentage FLOAT64, 
      cpu_utilization_mean_percentage FLOAT64,
      memory_utilization FLOAT64, 
      FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
    ) OPTIONS (description = 'Table for storing GCSFUSE metrics extracted from listing benchmark tests');
"""
query_create_table_ls_metrics_persistent_disk = """
    CREATE OR REPLACE TABLE gcsfuse-intern-project-2023.performance_metrics.ls_metrics_persistent_disk(
      configuration_id INT64, 
      gcsfuse_flags STRING, 
      branch STRING, 
      test_description STRING, 
      command STRING, 
      num_files INT64, 
      num_folder INT64, 
      num_samples INT64, 
      mean_latency_msec FLOAT64, 
      meadian_latency_msec FLOAT64, 
      standard_dev_msec FLOAT64, 
      percentile_latency_0 FLOAT64, 
      percentile_latency_20 FLOAT64, 
      percentile_latency_50 FLOAT64, 
      percentile_latency_90 FLOAT64, 
      percentile_latency_95 FLOAT64, 
      percentile_latency_98 FLOAT64, 
      percentile_latency_99 FLOAT64, 
      percentile_latency_99pt5 FLOAT64, 
      percentile_latency_99pt9 FLOAT64, 
      percentile_latency_100 FLOAT64, 
      cpu_utilization_peak_percentage FLOAT64, 
      cpu_utilization_mean_percentage FLOAT64, 
      memory_utilization FLOAT64, 
      FOREIGN KEY(configuration_id) REFERENCES performance_metrics.experiment_configuration (configuration_id) NOT ENFORCED
    ) OPTIONS (description = 'Table for storing PERSISTENT DISK metrics extracted from listing benchmark tests');
"""
results = client.query(query_create_table_experiment_configuration)
print(results)
results = client.query(query_create_table_fio_metrics)
print(results)
results = client.query(query_create_table_vm_metrics)
print(results)
results = client.query(query_create_table_ls_metrics_gcsfuse)
print(results)
results = client.query(query_create_table_ls_metrics_persistent_disk)
print(results)

query_insert_configuration = """
    INSERT INTO gcsfuse-intern-project-2023.performance_metrics.experiment_configuration VALUES ('--implicit-dirs --max-conns-per-host 100 --enable-storage-client-library --debug_fuse --debug_gcs --log-file $LOG_FILE --log-format \"text\" --stackdriver-export-interval=30s', 'master', 'TIMESTAMP("2023-12-25 05:30:00+00")', 'ruchikasharmaa-testing-vm-1');
"""
results = client.query(query_insert_configuration)
print(results)


