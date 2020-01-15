import argparse
import os
import logging
import sys
import json
import pandas as pd
import numpy as np
import time
import copy

from cromwell_tools.cromwell_api import CromwellAPI

from wdl_input_tools.helpers import configure_logging
import wdl_input_tools.core as wdl
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.contants as const


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="get_batch_status")

    # Output prefix
    argparser_obj.add_argument("--batch-name",
                               action="store",
                               type=str,
                               dest="batch_name",
                               required=True,
                               help="Batch name. Will return all workflows where cromwell-batch-name-label is this batch-name")

    # Output prefix
    argparser_obj.add_argument("--output-prefix",
                               action="store",
                               type=str,
                               dest="output_prefix",
                               required=True,
                               help="Output prefix where batch input, label, and cromwell status output files will be generated.")

    # Output prefix
    argparser_obj.add_argument("--show-excluded",
                               action="store_true",
                               dest="show_excluded",
                               help="Show all workflows in batch regardless of whether they're excluded")

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
    batch_name = args.batch_name
    output_prefix = args.output_prefix
    show_excluded = args.show_excluded
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = "http://"+cromwell_url.rpartition('/')[-1]

    # Configure logging appropriate for verbosity
    configure_logging(args.verbosity_level)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Otherwise just grab all of the workflows with batch-name
    batch_wfs = cromwell.query_workflows(auth, {"label": {const.CROMWELL_BATCH_LABEL: batch_name}})

    # Error out if batch doesn't actually exist
    if not batch_wfs:
        logging.error("No batch exists on current cromwell server with batch-name '{0}'".format(batch_name))
        raise IOError

    logging.info("Fetching workflow metadata...")
    wf_summaries = []
    for batch_wf in batch_wfs:
        wf_summaries.append(cromwell.get_wf_summary(auth, batch_wf))


    logging.info("Writing workflow report...")
    report_file = "{0}.batch_status.{1}.xlsx".format(output_prefix, time.strftime("%Y%m%d-%H%M%S"))

    report_df = pd.DataFrame(data=wf_summaries)

    # Reorder columns in a standard order
    report_df = report_df[const.STATUS_COL_ORDER]

    num_samples = len(report_df[const.CROMWELL_SAMPLE_LABEL].unique())
    num_success = len(report_df[report_df[const.CROMWELL_STATUS_FIELD] == const.CROMWELL_SUCCESS_STATUS][const.CROMWELL_SAMPLE_LABEL].unique())
    logging.info("{0}/{1} ({2}%) samples in batch have completed successfully!".format(num_success,
                                                                                       num_samples,
                                                                                       num_success/(1.0*num_samples)))

    # Remove workflows not in active batch unless otherwise specified
    if not show_excluded:
        report_df = report_df[report_df[const.CROMWELL_BATCH_STATUS_FIELD] == const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG]

    # Sort by sample name
    report_df = report_df.sort_values(by=const.CROMWELL_SAMPLE_LABEL)

    # Write to separate files
    report_df.to_excel(report_file, index=False)


if __name__ == "__main__":
    sys.exit(main())

