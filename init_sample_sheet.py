import argparse
import logging
import sys
import pandas as pd

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


def get_batch_wfs(prior_batch_name, auth):
    # Get workflows in batch to import from
    query = {"label": {const.CROMWELL_BATCH_LABEL: prior_batch_name,
                       const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG}}
    batch_wfs = cromwell.query_workflows(auth, query)
    # Raise error if no workflows found in batch
    if not batch_wfs:
        err_msg = "Batch name '{0}' doesn't exist on cromwell server!".format(prior_batch_name)
        logging.error(err_msg)
        raise IOError(err_msg)
    return batch_wfs


def main():

    # Configure argparser
    argparser = get_argparser()

    # Parse the arguments
    args = argparser.parse_args()

    # Input files: json input file to be used as template and
    batch_config_file = args.batch_config_file
    ss_output = args.sample_sheet_file
    optional_cols = args.optional_cols
    prior_batch_name = args.batch_name
    cromwell_url = args.cromwell_url

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Read in WDL template from JSON file
    batch_config = wdl.BatchConfig(batch_config_file)
    wdl_template = batch_config.batch_wdl_template

    # Get additional columns to include
    optional_cols = [x.strip() for x in optional_cols.split(",") if x != ""] if optional_cols != "ALL" else wdl_template.optional_cols
    logging.debug("Optional columns: {0}".format(optional_cols))
    if optional_cols:
        logging.info("Optional columns to include: {0}".format(", ".join(optional_cols)))

    # Initialize sample sheet that imports inputs from previous workflow batch
    if wdl_template.imports_from_previous_batch:
        logging.info("WDL template detects inputs that should be imported from previous batch!")

        # Raise error if imports are required but no batch name or cromwell server given on command line
        if prior_batch_name is None or cromwell_url is None:
            err_msg = "Must provide batch name and cromwell_url when WDL template imports from previous batch!"
            logging.error(err_msg)
            raise IOError(err_msg)

        # Authenticate and validate cromwell server
        cromwell_url = utils.fix_url(cromwell_url)
        auth = cromwell.get_cromwell_auth(url=cromwell_url)
        cromwell.validate_cromwell_server(auth)

        # Get batch of workflows to import values from
        import_wfs = get_batch_wfs(prior_batch_name, auth)

        # Init sample sheet with as many samples as batch
        sample_sheet = wdl.init_sample_sheet(wdl_template,
                                             optional_cols=optional_cols,
                                             num_samples=len(import_wfs))

        # Get columns in sample sheet that need to be imported from previous workflows
        cols_to_import = {"inputs": wdl_template.batch_input_cols,
                          "outputs": wdl_template.batch_output_cols,
                          "labels": wdl_template.batch_label_cols}

        # Populate sample sheet with values from imported workflows
        for i in range(len(import_wfs)):
            if i % 20 == 0:
                logging.info("Processed {0} imported workflows...".format(i))

            # Get workflow data from cromwell server
            wf = cromwell.BatchWorkflow(import_wfs[i], auth)
            wf.sync_metadata()

            # Populate fields in sample sheet that need to be imported
            for import_type, import_map in cols_to_import.items():
                for curr_wf_key, import_wf_key in import_map.items():
                    if import_type != "labels" and not wf.status == const.CROMWELL_SUCCESS_STATUS:
                        import_val = "UNSUCCESSFUL/UNFINISHED WORKFLOW"
                    else:
                        import_val = utils.get_dict_val(wf.metadata[import_type],
                                                        import_wf_key,
                                                        err_msg="Imported workflow {0} does not have any {1} with "
                                                                "required key: {2}".format(wf.wf_id,
                                                                                           import_type,
                                                                                           import_wf_key))
                    # Set sample sheet value to value returned from imported workflow
                    sample_sheet[curr_wf_key][i] = import_val

    # Initialize sample sheet from WDL template only
    else:
        # Initialize sample sheet
        sample_sheet = wdl.init_sample_sheet(wdl_template, optional_cols=optional_cols)

    # Write sample sheet to excel
    sample_sheet = pd.DataFrame(sample_sheet)
    sample_sheet.to_excel(ss_output, index=False)

    logging.info("Successfully initailized sample sheet to: {0}".format(ss_output))

if __name__ == "__main__":
    sys.exit(main())
