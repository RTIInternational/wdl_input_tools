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
    argparser_obj = argparse.ArgumentParser(prog="init_batch_sample_sheet")

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
    argparser_obj.add_argument("--inputs",
                               action="store",
                               type=file_type,
                               dest="input_json",
                               required=True,
                               help="Path to batch JSON inputs")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--labels",
                               action="store",
                               type=file_type,
                               dest="label_json",
                               required=True,
                               help="Path to batch JSON label file")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--wdl",
                               action="store",
                               type=file_type,
                               dest="wdl_workflow",
                               required=True,
                               help="Path to wdl workflow to be executed on batch")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--imports",
                               action="store",
                               type=file_type,
                               dest="wdl_imports",
                               required=True,
                               help="Path to zipped WDL imports for wdl workflow")

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


def log_submissions(submit_report, output_prefix, failed_workflow_index=None):

    # Write specify file
    logging.info("Writing workflow report...")
    report_file = "{0}.submit_batch.{1}.xlsx".format(output_prefix, time.strftime("%Y%m%d-%H%M%S"))

    report_df = pd.DataFrame(data=submit_report)

    # Add null values for column in boundary case where no workflows succeeded
    if not const.CROMWELL_WF_ID_FIELD in report_df.columns:
        report_df[const.CROMWELL_WF_ID_FIELD] = np.nan
    if not const.SUPERCEDED_WF_FIELD in report_df.columns:
        report_df[const.SUPERCEDED_WF_FIELD] = np.nan

    # Reorder columns in a standard order
    report_df = report_df[const.REPORT_COL_ORDER]

    # Count number of successes
    num_success = len(report_df[~report_df[const.CROMWELL_WF_ID_FIELD].isna()])

    # Fill in NA values from skipped workflows if error occured mid batch to indicate theses workflows weren't submitted
    report_df.fillna("SKIPPED")

    if failed_workflow_index is not None:
        failed_col = [""]*len(report_df)
        failed_col[failed_workflow_index] = "FAILED WORKFLOW"
        report_df["Failed"] = pd.Series(failed_col)

    logging.info("Successfully submitted {0} out of {1} workflows in batch!".format(num_success,
                                                                                    len(report_df)))

    # Write to separate files
    report_df.to_excel(report_file, index=False)


def main():

    # Configure argparser
    argparser = get_argparser()

    # Parse the arguments
    args = argparser.parse_args()

    # Input files: json input file to be used as template and
    batch_input_json = args.input_json
    batch_label_json = args.label_json
    wdl_workflow = args.wdl_workflow
    wdl_imports = args.wdl_imports
    output_prefix = args.output_prefix
    force_overwrite = args.force_overwrite
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = "http://"+cromwell_url.rpartition('/')[-1]

    # Configure logging appropriate for verbosity
    configure_logging(args.verbosity_level)

    # Read in batch inputs
    with open(batch_input_json, "r") as fh:
        batch_inputs = json.load(fh)

    with open(batch_label_json, "r") as fh:
        batch_labels = json.load(fh)

    assert len(batch_inputs) == len(batch_labels), "Batch label and input files are different sizes!"

    # Check to make sure all workflow labels have required label keys
    for wf_labels in batch_labels:
        wdl.validate_wf_labels(wf_labels)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Check for batch/sample conflicts and error out if --force option not used
    logging.info("Checking to see if any duplicate samples already exist in batch")
    batch_sample_labels = [wf_labels[const.CROMWELL_BATCH_SAMPLE_LABEL] for wf_labels in batch_labels]
    batch_conflicts = cromwell.get_batch_conflicts(auth, batch_sample_labels)
    if not force_overwrite and batch_conflicts:
        base_err_msg = "Cannot submit batch because one or more sample names conflicts with sample already in batch!"
        logging.error(base_err_msg)
        logging.error("Batch-sample conflicts: {0}".format(", ".join(batch_conflicts.keys())))
        raise IOError(base_err_msg)
    else:
        logging.warning("There are {0} batch/sample conflicts! See above logs for details.".format(len(batch_conflicts)))

    # Run workflows and exclude any conflicting workflows from current batch
    submit_report = copy.deepcopy(batch_labels)
    for i in range(len(batch_inputs)):
        wf_input = batch_inputs[i]
        wf_labels = batch_labels[i]
        batch_sample_label = wf_labels[const.CROMWELL_BATCH_SAMPLE_LABEL]

        # Submit workflow
        try:
            wf_id = cromwell.submit_wf_from_dict(auth,
                                                wdl_workflow,
                                                input_dict=wf_input,
                                                dependencies=wdl_imports,
                                                label_dict=wf_labels)

            # Update information for report
            submit_report[i][const.CROMWELL_WF_ID_FIELD] = wf_id
            wf_conflicts = [""] if batch_sample_label not in batch_conflicts else batch_conflicts[batch_sample_label]
            submit_report[i][const.SUPERCEDED_WF_FIELD] = ", ".join(wf_conflicts)

            if batch_sample_label not in batch_conflicts:
                continue

            # Change labels on conflicting workflows to 'exclude' to remove from active batch
            for batch_conflict_wf in wf_conflicts:
                # Exclude conflicting workflow from batch
                logging.warning("Excluding wf from batch due to new sample: {0}".format(batch_conflict_wf))
                cromwell.update_wf_batch_status(auth, batch_conflict_wf, include_in_batch=False)

                # Abort workflows if they're currently running
                conflict_wf_status = cromwell.get_wf_status(auth, batch_conflict_wf)
                if conflict_wf_status == const.CROMWELL_RUNNING_STATUS:
                    CromwellAPI.abort(batch_conflict_wf, auth, raise_for_status=True)
                    logging.warning("Aborted conflicting wf '{0}' "
                                    "with duplicate batch_sample_id '{1}'".format(batch_conflict_wf,
                                                                                  batch_sample_label))


        except BaseException:
            # Log any successful submissions and indicate workflow which caused failure
            log_submissions(submit_report, output_prefix, failed_workflow_index=i)
            raise

    # Log submission if all workflows submitted successfully
    logging.info("Successfully submitted all workflows in batch!")
    log_submissions(submit_report, output_prefix)


if __name__ == "__main__":
    sys.exit(main())
