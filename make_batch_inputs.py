import argparse
import os
import logging
import sys
import pandas as pd
import json

from wdl_input_tools.helpers import configure_logging
import wdl_input_tools.core as wdl
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.helpers as utils


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="make_batch_inputs")

    def file_type(arg_string):
        """
        This function check both the existance of input file and the file size
        :param arg_string: file name as string
        :return: file name as string
        """
        if not os.path.exists(arg_string):
            err_msg = "%s does not exist! " \
                      "Please provide a valid file!" % arg_string
            raise argparse.ArgumentTypeError(err_msg)

        return arg_string

    # Path to batch config file
    argparser_obj.add_argument("--batch-config",
                               action="store",
                               type=file_type,
                               dest="batch_config_file",
                               required=True,
                               help="Path to batch config yaml file")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--sample-sheet",
                               action="store",
                               type=file_type,
                               dest="sample_sheet_file",
                               required=True,
                               help="Path to sample sheet that will be used to populate WDL template")

    # Batch name to be associated with all workflows
    argparser_obj.add_argument("--batch-name",
                               action="store",
                               type=str,
                               dest="batch_name",
                               required=True,
                               help="Name to associate with batch of workflows")

    # Output prefix
    argparser_obj.add_argument("--output-prefix",
                               action="store",
                               type=str,
                               dest="output_prefix",
                               required=True,
                               help="Output prefix where batch input, label, and cromwell status output files will be generated.")

    # Output prefix
    argparser_obj.add_argument("--force",
                               action="store_true",
                               dest="force_overwrite",
                               help="Flag to disable batch-name uniqueness checking.")

    # Cromwell server IP address
    argparser_obj.add_argument("--cromwell-url",
                               action="store",
                               type=str,
                               dest="cromwell_url",
                               required=True,
                               help="URL for connecting to Cromwell server (IP + Port; e.g. 10.12.154.0:8000)")

    # Verbosity level
    argparser_obj.add_argument("-v",
                               action='count',
                               dest='verbosity_level',
                               required=False,
                               default=0,
                               help="Increase verbosity of the program."
                                    "Multiple -v's increase the verbosity level:\n"
                                    "0 = Errors\n"
                                    "1 = Errors + Warnings\n"
                                    "2 = Errors + Warnings + Info\n"
                                    "3 = Errors + Warnings + Info + Debug")

    return argparser_obj

def main():

    # Configure argparser
    argparser = get_argparser()

    # Parse the arguments
    args = argparser.parse_args()

    # Input files: json input file to be used as template and
    batch_config_file = args.batch_config_file
    ss_file  = args.sample_sheet_file
    batch_name = args.batch_name
    output_prefix = args.output_prefix
    force_overwrite = args.force_overwrite
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = "http://"+cromwell_url.rpartition('/')[-1]

    # Configure logging appropriate for verbosity
    configure_logging(args.verbosity_level)

    # Read in batch configuration options from yaml file
    batch_config = wdl.BatchConfig(batch_config_file)

    # Read in sample sheet from excel file
    sample_sheet = wdl.InputSampleSheet(ss_file, sample_id_col=batch_config.sample_id_col)

    # Validate sample sheet using validation functions specified in batch config file
    batch_config.validate_sample_sheet(sample_sheet)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Throw error if batch name is not unique
    if not cromwell.is_unique_batch_name(auth, batch_name) and not force_overwrite:
        err_msg = "Batch name '{0}' is not unique! Pick another name for this batch".format(batch_name)
        logging.error(err_msg)
        raise IOError(err_msg)

    # Write batch output files
    batch_inputs_file = "{0}.inputs.json".format(output_prefix)
    batch_labels_file = "{0}.labels.json".format(output_prefix)
    batch_status_file = "{0}.batch_report.xlsx".format(output_prefix)

    # Write batch input json
    logging.info("Making JSON input file for batch...")
    inputs_json = wdl.make_batch_inputs(sample_sheet, batch_config.batch_wdl_template)
    with open(batch_inputs_file, "w") as fh:
        json.dump(inputs_json, fh, indent=1, cls=utils.NpEncoder)

    # Write batch label json
    logging.info("Making JSON label file for batch...")
    labels_json = wdl.make_batch_labels(sample_sheet, batch_config.batch_wdl_template, batch_name)
    with open(batch_labels_file, "w") as fh:
        json.dump(labels_json, fh, indent=1, cls=utils.NpEncoder)

    # Write batch status sheet
    logging.info("Making cromwell status sheet for batch...")
    status_df = pd.DataFrame(data=labels_json)
    status_df.to_excel(batch_status_file, index=False)


if __name__ == "__main__":
    sys.exit(main())
