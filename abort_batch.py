import argparse
import logging
import sys
import requests

from cromwell_tools.cromwell_api import CromwellAPI

import wdl_input_tools.helpers as utils
import wdl_input_tools.cromwell as cromwell
import wdl_input_tools.contants as const


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="abort_batch")

    # Batch name to abort
    argparser_obj.add_argument("--batch-name",
                               action="store",
                               type=str,
                               dest="batch_name",
                               required=True,
                               help="Batch name. Will return all workflows where cromwell-batch-name-label is this batch-name")

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
    cromwell_url = args.cromwell_url

    # Standardize url
    cromwell_url = utils.fix_url(cromwell_url)

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Authenticate and validate cromwell server
    auth = cromwell.get_cromwell_auth(url=cromwell_url)
    cromwell.validate_cromwell_server(auth)

    # Otherwise just grab all of the workflows with batch-name
    batch_wfs = cromwell.query_workflows(auth, {"label": {const.CROMWELL_BATCH_LABEL: batch_name}})

    # Error out if batch doesn't actually exist
    if not batch_wfs:
        logging.error("No batch exists on current cromwell server with batch-name '{0}'".format(batch_name))
        raise IOError

    # Terminal wf status codes
    terminal_states = [const.CROMWELL_ABORTING_STATUS,
                       const.CROMWELL_ABORTED_STATUS,
                       const.CROMWELL_SUCCESS_STATUS,
                       const.CROMWELL_FAILED_STATUS]

    logging.info("Aborting workflows...")
    aborted_wfs = 0
    running_wfs = 0
    for wf in batch_wfs:
        wf_status = cromwell.get_wf_status(auth, wf)
        if wf_status not in terminal_states:
            try:
                logging.info("Aborting wf: {0}".format(wf))
                CromwellAPI.abort(wf, auth, raise_for_status=True)
                aborted_wfs += 1
            except requests.exceptions.HTTPError:
                logging.warning("Unable to abort wf '{0}' for some reason...".format(wf))
            finally:
                running_wfs += 1

    success_rate = 0.0 if running_wfs == 0 else (aborted_wfs/(1.0*running_wfs))*100
    logging.info("{0}/{1} ({2}%) pending batch workflows successfully aborted!".format(aborted_wfs,
                                                                                       running_wfs,
                                                                                       success_rate))


if __name__ == "__main__":
    sys.exit(main())

