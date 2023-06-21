import bigquery

PROJECT_ID = 'gcs-fuse-test'
DATASET_ID = 'performance_metrics'

if __name__ == '__main__':
  bigquery_obj = bigquery.BigQuery(PROJECT_ID, DATASET_ID)
  bigquery_obj.setup_bigquery()