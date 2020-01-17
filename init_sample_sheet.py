import argparse
import logging
import sys
import pandas as pd

from cromwell_tools.cromwell_api import CromwellAPI

import wdl_input_tools.helpers as utils
import wdl_input_tools.core as wdl
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.contants as const
import wdl_input_tools.cli as cli


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="init_batch_sample_sheet")

    # Path to batch config file that defines input template, validation classes, and merge instructions
    argparser_obj.add_argument("--batch-config",
                               action="store",
                               type=cli.file_type_arg,
                               dest="batch_config_file",
                               required=True,
                               help="Path to batch config file")

    # Output path where sample sheet template will be written
    argparser_obj.add_argument("--sample-sheet",
                               action="store",
                               type=cli.excel_type_arg,
                               dest="sample_sheet_file",
                               required=True,
                               help="Path to output file where sample sheet will be initialized")

    # List of optional values to include in sample sheet template
    # Default behavior is to output only required columns to sample sheet template
    argparser_obj.add_argument("--optional-cols",
                               action="store",
                               type=str,
                               dest="optional_cols",
                               required=False,
                               default='',
                               help="Comma-separated list of additional columns to include. "
                                    "'ALL' indicates all columns should be included in output samplesheet")

    # Batch name to be associated with all workflows
    argparser_obj.add_argument("--populate-from-batch",
                               action="store",
                               type=cli.batch_type_arg,
                               dest="batch_name",
                               required=False,
                               help="Name to batch to pull workflow inputs from if WF inputs required by WDL template")

    # Cromwell server IP address
    argparser_obj.add_argument("--cromwell-url",
                               action="store",
                               type=str,
                               dest="cromwell_url",
                               required=False,
                               help="URL for connecting to Cromwell server (IP + Port; e.g. 10.12.154.0:8000) "
                                    "if pulling inputs from another batch")

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


def import_workflow_inputs(auth, batch_name, wdl_template):
    # Import workflow outputs specified in WDLTemplate to current workflow

    # Get workflows in batch
    query = {"label": {const.CROMWELL_BATCH_LABEL: batch_name,
                       const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG}}
    batch_wfs = cromwell.query_workflows(auth, query)

    # Raise error if no workflows found in batch
    if not batch_wfs:
        err_msg = "Batch name '{0}' doesn't exist on cromwell server!".format(batch_name)
        logging.error(err_msg)
        raise IOError(err_msg)

    # Get output files from each wf based on values specified in wf template
    sample_names = []
    wf_outputs = {key: [] for key in wdl_template.batch_import_keys}
    for wf in batch_wfs:
        metadata = CromwellAPI.metadata(wf, auth, includeKey=["outputs", "labels", "status"],
                                        raise_for_status=True).json()
        wf_status = metadata["status"]
        sample_name = metadata["labels"][const.CROMWELL_SAMPLE_LABEL]

        # Check to make sure sample name is unique in batch
        if sample_name in sample_names:
            err_msg = "Duplicate samples in batch: {0}! Samples must be unique. " \
                      "Update batch status of non-unique sample runs!".format(sample_name)
            logging.error(err_msg)
            raise IOError(err_msg)

        if wf_status != const.CROMWELL_SUCCESS_STATUS:
            # Add null values for outputs from workflows that haven't or didn't succeed
            # Let user decide what to do about these in the downstream sample sheet (exclude or rerun sample)
            outputs = {key: "UNSUCCESSFUL/UNFINISHED WORKFLOW" for key in wdl_template.batch_output_cols}
        else:
            # Get output files from metadata using the keys specified in the WDL template
            # E.g. @@@rnaseq_pe_wf.fastqc_file will look for an output with key 'rnaseq_pe_wf.fastqc_file'
            try:
                outputs = {key: metadata["outputs"][val] for key, val in wdl_template.batch_output_cols.items()}
            except KeyError:
                logging.error("Successful workflow '{0}' did not produce one "
                              "or more outputs file keys specified in WDL template!".format(wf))
                raise

        # Get workflow labels specified in config
        try:
            labels = {key: metadata["labels"][val] for key, val in wdl_template.batch_label_cols.items()}
        except KeyError:
            logging.error("Successful workflow '{0}' did not have one "
                          "or more workflow labels specified in WDL template!".format(wf))
            raise

        # Add output files from workflow to full set of output files
        for wdl_template_key, wf_output in outputs.items():
            wf_outputs[wdl_template_key].append(wf_output)

        # Add labels from workflow to full set of labels
        for wdl_template_key, wf_label in labels.items():
            wf_outputs[wdl_template_key].append(wf_label)

        # Add sample name to list of seen samples
        sample_names.append(sample_name)

    return wf_outputs


def main():

    # Configure argparser
    argparser = get_argparser()

    # Parse the arguments
    args = argparser.parse_args()

    # Input files: json input file to be used as template and
    batch_config_file = args.batch_config_file
    ss_output = args.sample_sheet_file
    optional_cols = args.optional_cols
    batch_name = args.batch_name
    cromwell_url = args.cromwell_url

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Read in WDL template from JSON file
    batch_config = wdl.BatchConfig(batch_config_file)
    wdl_template = batch_config.batch_wdl_template

    # Authenticate to server and validate batch name
    wf_outputs = {}
    if wdl_template.imports_from_batch:
        logging.info("WDL template detects inputs that should be imported from previous batch!")
        # Raise error if imports are required but no batch name or cromwell server given on command line
        if batch_name is None or cromwell_url is None:
            err_msg = "Must provide batch name and cromwell_url when WDL template imports from previous batch!"
            logging.error(err_msg)
            raise IOError(err_msg)

        # Authenticate and validate cromwell server
        cromwell_url = utils.fix_url(cromwell_url)
        auth = cromwell.get_cromwell_auth(url=cromwell_url)
        cromwell.validate_cromwell_server(auth)

        # Grab outputs from batch workflows that match input keys specified in WDL template
        wf_outputs = import_workflow_inputs(auth, batch_name, wdl_template)

    # Get additional columns to include
    optional_cols = [x.strip() for x in optional_cols.split(",") if x != ""] if optional_cols != "ALL" else wdl_template.optional_cols
    logging.debug("Optional columns: {0}".format(optional_cols))

    if optional_cols:
        logging.info("Optional columns to include: {0}".format(", ".join(optional_cols)))

    # Initialize sample sheet
    num_samples = 1 if not wf_outputs else len(pd.DataFrame(wf_outputs))
    sample_sheet = wdl.init_sample_sheet(wdl_template,
                                         optional_cols=optional_cols,
                                         num_samples=num_samples)

    # Replace columns in template sample sheet with outputs imported from previous workflow batch
    if wf_outputs:
        wf_outputs = pd.DataFrame(wf_outputs)
        for col in wf_outputs.columns:
            sample_sheet[col] = wf_outputs[col]

    # Write to excel
    sample_sheet.to_excel(ss_output, index=False)

    logging.info("Successfully initailized sample sheet to: {0}".format(ss_output))

if __name__ == "__main__":
    sys.exit(main())
