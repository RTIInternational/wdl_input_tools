import argparse
import logging
import sys
import pandas as pd
import time

import wdl_input_tools.helpers as utils
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.contants as const
import wdl_input_tools.cli as cli


def get_argparser():

    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="get_batch_status")

    # Name of batch from which to fetch status updates
    argparser_obj.add_argument("--batch-name",
                               action="store",
                               type=cli.batch_type_arg,
                               dest="batch_name",
                               required=True,
                               help="Batch name. Will return all workflows where cromwell-batch-name-label is this batch-name")

    # Output prefix for status output file
    argparser_obj.add_argument("--output-prefix",
                               action="store",
                               type=cli.prefix_type_arg,
                               dest="output_prefix",
                               required=True,
                               help="Output prefix where batch input, label, and cromwell status output files will be generated.")

    # Whether to show status of batch workflows excluded from the active batch (batch_status == excluded)
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


def get_status_count(report_df, status):
    return len(report_df[report_df[const.CROMWELL_STATUS_FIELD] == status][const.CROMWELL_SAMPLE_LABEL])


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
    cromwell_url = utils.fix_url(cromwell_url)

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Grab workflows from batch
    query = {"label": {const.CROMWELL_BATCH_LABEL: batch_name}}
    if not show_excluded:
        # Optinally get only workflows in active batch (batch_status = include)
        query["label"][const.CROMWELL_BATCH_STATUS_FIELD] = const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG

    batch_wfs = cromwell.query_workflows(auth, query)

    # Error out if batch doesn't actually exist
    if not batch_wfs:
        logging.error("No batch exists on current cromwell server with batch-name '{0}'".format(batch_name))
        raise IOError

    logging.info("Fetching workflow metadata...")
    wf_summaries = []
    count = 0
    for batch_wf in batch_wfs:
        wf_summaries.append(cromwell.get_wf_summary(auth, batch_wf))
        count += 1
        if count % 20 == 0:
            logging.info("Processed {0} workflows...".format(count))

    logging.info("Creating workflow report...")
    report_df = pd.DataFrame(data=wf_summaries)

    # Reorder columns in a standard order
    # Remove columns if they don't exist in df
    # e.g. - if you just submitted a batch and none have finished, 'end' won't be a label in the df
    col_order = [x for x in const.STATUS_COL_ORDER if x in report_df.columns]
    report_df = report_df[col_order]

    # Report status of samples in either current active batch (batch_status = include) or all workflows in batch
    report_type = "complete batch (include+exclude)"
    if not show_excluded:
        report_df = report_df[report_df[const.CROMWELL_BATCH_STATUS_FIELD] == const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG]
        report_type = "active batch"

    num_samples = len(report_df)
    for status in const.WF_STATUS_VALS:
        status_count = get_status_count(report_df, status)
        logging.info("{0}/{1} ({2}%) samples {3} in {4}!".format(status_count,
                                                                 num_samples,
                                                                 (status_count / (1.0 * num_samples)) * 100,
                                                                 status,
                                                                 report_type))

    # Sort by sample name
    report_df = report_df.sort_values(by=const.CROMWELL_SAMPLE_LABEL)

    # Write to separate files
    report_file = "{0}.batch_status.{1}.xlsx".format(output_prefix, time.strftime("%Y%m%d-%H%M%S"))
    report_df.to_excel(report_file, index=False)


if __name__ == "__main__":
    sys.exit(main())

