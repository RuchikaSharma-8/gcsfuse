#!/bin/bash
set -e
echo "Mounting gcs bucket"
mkdir -p gcs
GCSFUSE_FLAGS=$1
BUCKET_NAME=periodic-perf-experiments
MOUNT_POINT=gcs
# The VM will itself exit if the gcsfuse mount fails.
gcsfuse $GCSFUSE_FLAGS $BUCKET_NAME $MOUNT_POINT

echo Print the time when FIO tests start
date
echo Running fio test..
fio job_files/seq_rand_read_write.fio --lat_percentiles 1 --output-format=json --output='fio-output.json'

BRANCH=$2
END_DATE=$3
echo Installing requirements..
pip install --require-hashes -r requirements.txt --user
#gsutil cp gs://periodic-perf-tests/creds.json gsheet
echo Fetching results..
# Upload data to the gsheet only when it runs through kokoro.
#if [ "${KOKORO_JOB_TYPE}" != "RELEASE" ] && [ "${KOKORO_JOB_TYPE}" != "CONTINUOUS_INTEGRATION" ] && [ "${KOKORO_JOB_TYPE}" != "PRESUBMIT_GITHUB" ];
#then
python3 fetch_metrics.py fio-output.json
#else
  #python3 fetch_metrics.py fio-output.json --gcsfuse_flags "$GCSFUSE_FLAGS" --branch "$BRANCH" --end_date "$END_DATE" --upload
#fi

sudo umount $MOUNT_POINT
