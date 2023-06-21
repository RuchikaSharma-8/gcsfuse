import bigquery
import bigquery_setup
import argparse
import sys

def parse_arguments(argv):
  """Parses the arguments provided to the script via command line.

  Args:
    argv: List of arguments received by the script.

  Returns:
    A class containing the parsed arguments.
  """
  argv = sys.argv
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--gcsfuse_flags',
      help='Set of GCSFuse flags.',
      action='store',
      nargs=1,
      required=True
  )
  parser.add_argument(
      '--branch',
      help='GCSFuse repo branch used for building GCSFuse.',
      action='store',
      nargs=1,
      required=True
  )
  parser.add_argument(
      '--end_date',
      help='Date upto when tests are run.',
      action='store',
      nargs=1,
      required=True
  )
  parser.add_argument(
      '--config_name',
      help='Name of the experiment configuration.',
      action='store',
      nargs=1,
      required=True
  )
  return parser.parse_args(argv[1:])

if __name__ == '__main__':
  argv = sys.argv
  args = parse_arguments(argv)
  bigquery_obj = bigquery.ExperimentsGCSFuseBQ(bigquery_setup.PROJECT_ID, bigquery_setup.DATASET_ID)
  exp_config_id = bigquery_obj.get_experiment_configuration_id(args.gcsfuse_flags[0], args.branch[0], args.end_date[0], args.config_name[0])