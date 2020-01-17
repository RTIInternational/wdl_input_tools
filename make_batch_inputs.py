import argparse
import logging
import sys
import pandas as pd
import json
import time
import shutil

import wdl_input_tools.core as wdl
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.helpers as utils
import wdl_input_tools.cli as cli


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="make_batch_inputs")

    # Path to batch config file
    argparser_obj.add_argument("--batch-config",
                               action="store",
                               type=cli.file_type_arg,
                               dest="batch_config_file",
                               required=True,
                               help="Path to batch config yaml file")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--sample-sheet",
                               action="store",
                               type=cli.file_type_arg,
                               dest="sample_sheet_file",
                               required=True,
                               help="Path to sample sheet that will be used to populate WDL template")

    # Batch name to be associated with all workflows
    argparser_obj.add_argument("--batch-name",
                               action="store",
                               type=cli.batch_type_arg,
                               dest="batch_name",
                               required=True,
                               help="Name to associate with batch of workflows")

    # Output prefix
    argparser_obj.add_argument("--output-dir",
                               action="store",
                               type=cli.dir_type_arg,
                               dest="output_dir",
                               required=True,
                               help="Output dir where batch input, label, and cromwell status output files will be generated.")

    # Option to override name-checking
    # If not specified, program will error out if batch_name is not unique to cromwell
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
    output_dir = args.output_dir
    force_overwrite = args.force_overwrite
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = utils.fix_url(cromwell_url)

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

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

    # Convert sample sheet to WDL json inputs
    wdl_template = batch_config.batch_wdl_template
    wf_name = wdl_template.workflow_name

    if batch_config.wf_type == "scatter":
        # Scatter workflows convert ss to list of jsons where each row is input to separate wf (e.g. RNAseq wf)
        logging.info("Generating workflow inputs json...")
        ss_inputs = sample_sheet.sample_sheet.to_dict(orient="records")
        inputs_json = [wdl_template.make_wf_input(ss_input) for ss_input in ss_inputs]
        logging.info("Generating workflow labels json...")
        labels_json = [wdl.get_wf_labels(sample, batch_name, wf_name) for sample in sample_sheet.sample_names]

    elif batch_config.wf_type == "gather":
        # Gather workflows convert ss to single json
        # Each column passed to one wf key as a list (e.g. merge RNAseq wf)
        logging.info("Generating workflow inputs json...")
        inputs_json = sample_sheet.sample_sheet.to_dict(orient="list")
        inputs_json = wdl_template.make_wf_input(inputs_json)
        logging.info("Generating workflow labels json...")
        labels_json = wdl.get_wf_labels(batch_name, batch_name, wf_name)

    # Write batch output files
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_prefix = "{0}/{1}".format(output_dir, batch_name)
    batch_inputs_file = "{0}.make_batch.inputs.{1}.json".format(output_prefix, timestamp)
    batch_labels_file = "{0}.make_batch.labels.{1}.json".format(output_prefix, timestamp)
    batch_status_file = "{0}.make_batch.report.{1}.xlsx".format(output_prefix, timestamp)
    batch_config_record_file = "{0}.make_batch.config.{1}.yaml".format(output_prefix, timestamp)

    # Write batch inputs to json file
    with open(batch_inputs_file, "w") as fh:
        json.dump(inputs_json, fh, indent=1, cls=utils.NpEncoder)

    # Write batch label json file
    with open(batch_labels_file, "w") as fh:
        json.dump(labels_json, fh, indent=1, cls=utils.NpEncoder)

    # Write batch status sheet
    logging.info("Making cromwell status sheet for batch...")
    if batch_config.wf_type == "gather":
        labels_json = {k: [v] for k,v in labels_json.items()}
    status_df = pd.DataFrame(data=labels_json)
    status_df.to_excel(batch_status_file, index=False)

    # Write record of batch config so we know how this set of inputs was created
    shutil.copy(batch_config_file, batch_config_record_file)


if __name__ == "__main__":
    sys.exit(main())
