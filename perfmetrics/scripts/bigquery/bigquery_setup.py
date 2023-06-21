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
"""Python script for creating the dataset and tables under the project in BigQuery.

This python script calls the bigquery module to create the dataset that will store the tables,
the table to store the experiment configurations and the tables to store the metrics data.
Note: BigQuery API should be enabled for the project
"""
import bigquery

PROJECT_ID = 'gcs-fuse-test'
DATASET_ID = 'performance_metrics'

if __name__ == '__main__':
  bigquery_obj = bigquery.BigQuery(PROJECT_ID, DATASET_ID)
  bigquery_obj.setup_dataset_and_tables()