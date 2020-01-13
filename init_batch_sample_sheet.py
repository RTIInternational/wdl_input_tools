import argparse
import os
import logging
import sys

from wdl_input_tools.helpers import configure_logging
import wdl_input_tools.core as wdl

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

    # Path to VCF input file
    argparser_obj.add_argument("--wdl-input",
                               action="store",
                               type=file_type,
                               dest="wdl_input_file",
                               required=True,
                               help="Path to WDL template")

    # Path to VCF input file
    argparser_obj.add_argument("--sample-sheet",
                               action="store",
                               type=str,
                               dest="sample_sheet_file",
                               required=True,
                               help="Path to output file where sample sheet will be initialized")

    # Path to VCF input file
    argparser_obj.add_argument("--optional-cols",
                               action="store",
                               type=str,
                               dest="optional_cols",
                               required=False,
                               default='',
                               help="Comma-separated list of additional columns to include. "
                                    "'ALL' indicates all columns should be included in output samplesheet")


    # Verbosity level
    argparser_obj.add_argument("-v",
                               action='count',
                               dest='verbosity_level',
                               required=False,
                               default=2,
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
    wdl_input = args.wdl_input_file
    ss_output = args.sample_sheet_file
    optional_cols = args.optional_cols

    # Configure logging appropriate for verbosity
    configure_logging(args.verbosity_level)

    # Read in WDL template from JSON file
    wdl_template = wdl.WDLInputTemplate(wdl_input)

    # Get additional columns to include
    optional_cols = [x.strip() for x in optional_cols.split(",") if x != ""] if optional_cols != "ALL" else wdl_template.optional_cols
    logging.debug("Optional columns: {0}".format(optional_cols))

    if optional_cols:
        logging.info("Optional columns to include: {0}".format(", ".join(optional_cols)))

    wdl.init_sample_sheet_file(wdl_template,
                               ss_output,
                               optional_cols=optional_cols)

    logging.info("Successfully initailized sample sheet to: {0}".format(ss_output))

if __name__ == "__main__":
    sys.exit(main())
