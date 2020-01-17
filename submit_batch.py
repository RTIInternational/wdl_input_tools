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

import wdl_input_tools.helpers as utils
import wdl_input_tools.core as wdl
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.contants as const
import wdl_input_tools.cli as cli


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="submit_batch")

    # Path to json containing batch wdl inputs
    argparser_obj.add_argument("--inputs",
                               action="store",
                               type=cli.file_type_arg,
                               dest="input_json",
                               required=True,
                               help="Path to batch JSON inputs")

    # Path to json containing batch wdl labels
    argparser_obj.add_argument("--labels",
                               action="store",
                               type=cli.file_type_arg,
                               dest="label_json",
                               required=True,
                               help="Path to batch JSON label file")

    # Path to wdl workflow that will be run on each sample
    argparser_obj.add_argument("--wdl",
                               action="store",
                               type=cli.file_type_arg,
                               dest="wdl_workflow",
                               required=True,
                               help="Path to wdl workflow to be executed on batch")

    # Path to zipped directory of wdl workflow dependencies
    argparser_obj.add_argument("--imports",
                               action="store",
                               type=cli.file_type_arg,
                               dest="wdl_imports",
                               required=True,
                               help="Path to zipped WDL imports for wdl workflow")

    # Output prefix
    argparser_obj.add_argument("--output-prefix",
                               action="store",
                               type=cli.prefix_type_arg,
                               dest="output_prefix",
                               required=True,
                               help="Output prefix where batch input, label, and cromwell status output files will be generated.")

    # Configure how to deal with batch conflicts (i.e. when a sample workflow has already been submitted/executed)
    # It's basically the re-run policy. On any given batch you can choose to re-run:
    # 1) only samples where previous workflow failed,
    # 2) only samples that have not yet succeeded (failed/running/submitted/pending; good for hung jobs),
    # 3) re-run everything in the batch regardless of any prior/ongoing workflows
    argparser_obj.add_argument("--batch-conflict-action",
                               action="store",
                               dest="batch_conflict_action",
                               type=str,
                               choices=["rerun-all", "rerun-unless-success", "rerun-failed"],
                               default="rerun-failed",
                               help="Action on batch conflict:\n"
                                    "rerun-failed = Only re-run samples if a previous wf failed\n"
                                    "rerun-unless-success = Re-run sample unless previous wf succeeded. "
                                    "Aborts prior running/submitted workflows\n"
                                    "rerun-all = Re-run all samples regardless of prior workflow success" )

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


def resolve_batch_conflict(auth, batch_conflict_wf, batch_conflict_action):
    # Applies rerun logic to determine whether to rerun a workflow for sample when one already exists

    # Whether the previous workflow can be replaced by another submission
    can_overwrite_batch_conflict = True

    # Get status of previous workflow
    batch_conflict_status = cromwell.get_wf_status(auth, batch_conflict_wf)
    batch_conflict_alive = batch_conflict_status not in [const.CROMWELL_FAILED_STATUS,
                                                         const.CROMWELL_SUCCESS_STATUS,
                                                         const.CROMWELL_ABORTED_STATUS,
                                                         const.CROMWELL_ABORTING_STATUS]

    # Do not overwrite successful workflows unless rerun-all option specified
    if batch_conflict_status == const.CROMWELL_SUCCESS_STATUS and batch_conflict_action != "rerun-all":
        can_overwrite_batch_conflict = False

    # Do not overwrite running/pending/submitted workflows unless rerun-unless-success option specified
    elif batch_conflict_alive and batch_conflict_action == "rerun-failed":
        can_overwrite_batch_conflict = False

    # Abort batch conflict workflow if it can be overwritten and is currently in a non-terminal state
    abort_batch_conflict = batch_conflict_alive and can_overwrite_batch_conflict
    return can_overwrite_batch_conflict, abort_batch_conflict


def output_job_report(job_report, report_file, failed_workflow_index=None):
    # Convert dict to a dataframe for easier munging
    report_df = pd.DataFrame(data=job_report)

    # Add null values for column in boundary case where no workflows succeeded
    for col in [const.CROMWELL_WF_ID_FIELD, const.SUPERCEDED_WF_FIELD, const.REPORT_INFO_FIELD]:
        if col not in report_df.columns:
            report_df[col] = np.nan

    # Reorder columns in a standard order
    report_df = report_df[const.REPORT_COL_ORDER]

    # Fill in NA values from skipped workflows if error occured mid batch to indicate theses workflows weren't submitted
    report_df.fillna("SKIPPED AFTER FAILURE")

    if failed_workflow_index is not None:
        failed_col = [""]*len(report_df)
        failed_col[failed_workflow_index] = "FAILED WORKFLOW"
        report_df["Failed"] = pd.Series(failed_col)

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
    batch_conflict_action = args.batch_conflict_action
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = utils.fix_url(cromwell_url)

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Read in batch inputs
    with open(batch_input_json, "r") as fh:
        batch_inputs = json.load(fh)

    with open(batch_label_json, "r") as fh:
        batch_labels = json.load(fh)

    # Convert inputs/lables to lists if they're not already lists
    batch_inputs = [batch_inputs] if not isinstance(batch_inputs, list) else batch_inputs
    batch_labels = [batch_labels] if not isinstance(batch_labels, list) else batch_labels

    assert len(batch_inputs) == len(batch_labels), "Batch label and input files are different sizes!"

    # Check to make sure all workflow labels have required label keys
    for wf_labels in batch_labels:
        wdl.validate_wf_labels(wf_labels)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Create a report to detail what jobs were run/skipped/failed
    job_report = copy.deepcopy(batch_labels)
    report_file = "{0}.submit_batch.{1}.xlsx".format(output_prefix, time.strftime("%Y%m%d-%H%M%S"))

    # Loop through workflows to see if they need to be run/rerun
    submitted_wfs = 0
    for i in range(len(batch_inputs)):
        # Get inputs, labels, and batch_sample label for next workflow in batch
        wf_input = batch_inputs[i]
        wf_labels = batch_labels[i]
        batch_sample_label = wf_labels[const.CROMWELL_BATCH_SAMPLE_LABEL]

        # Try to run the workflow
        try:

            # Get list of previous workflows in this batch with the same sample name (batch conflicts)
            batch_conflict_wfs = cromwell.get_batch_conflicts(auth, batch_sample_label)

            # Determine how to resolve each batch conflict
            can_submit_wf = True
            for batch_conflict_wf in batch_conflict_wfs:
                can_overide_conflict_wf, abort_conflict_wf = resolve_batch_conflict(auth,
                                                                                    batch_conflict_wf,
                                                                                    batch_conflict_action)

                # Exclude prior conflicting wf from the current active batch
                if can_overide_conflict_wf:
                    cromwell.update_wf_batch_status(auth, batch_conflict_wf, include_in_batch=False)

                # Abort prior conflicting wf if in non-terminal state (fail, success)
                if abort_conflict_wf:
                    CromwellAPI.abort(batch_conflict_wf, auth, raise_for_status=True)
                    logging.warning("Aborted conflicting wf '{0}' "
                                    "with duplicate batch_sample_id '{1}'".format(batch_conflict_wf,
                                                                                  batch_sample_label))

                # Workflow can only be submitted if it can override all prior workflows
                can_submit_wf = can_submit_wf and can_overide_conflict_wf

            # Workflow id of submission and message to print in job report file.
            # Will be overwritten if job is submitted.
            wf_id = "Not submitted"
            msg = "SupercededWF is either running/submitted or was successful."

            # Run the workflow if you can
            if can_submit_wf:

                # Show some logging stuff
                logging.info("Submitting workflow for '{0}'".format(batch_sample_label))
                if batch_conflict_wfs:
                    logging.warning("Superceded workflows: {0}".format(", ".join(batch_conflict_wfs)))

                # Submit workflow and get id of the newly submitted workflow
                wf_id = cromwell.submit_wf_from_dict(auth,
                                                     wdl_workflow,
                                                     input_dict=wf_input,
                                                     dependencies=wdl_imports,
                                                     label_dict=wf_labels)

                # Increment counter of successfully submitted workflows
                submitted_wfs += 1
                msg = ""

            # Update information for report
            job_report[i][const.CROMWELL_WF_ID_FIELD] = wf_id
            job_report[i][const.SUPERCEDED_WF_FIELD] = ", ".join(batch_conflict_wfs)
            job_report[i][const.REPORT_INFO_FIELD] = msg

        except BaseException:
            # Log any successful submissions and indicate workflow which caused failure
            logging.info("Successfully submitted {0} out of {1} workflows in batch!".format(submitted_wfs,
                                                                                            len(batch_inputs)))
            logging.info("Writing workflow report...")
            output_job_report(job_report, report_file, failed_workflow_index=i)
            raise

    # Log submission if all workflows submitted successfully
    logging.info("Successfully submitted {0} out of {1} workflows in batch!".format(submitted_wfs,
                                                                                    len(batch_inputs)))
    logging.info("Writing workflow report...")
    output_job_report(job_report, report_file)


if __name__ == "__main__":
    sys.exit(main())
