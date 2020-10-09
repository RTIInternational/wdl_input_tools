#!/usr/bin/env python3

import argparse
import logging
import sys
import pandas as pd
import json
import time
import shutil

import wdl_input_tools.helpers as utils
import wdl_input_tools.cli as cli


def get_argparser():
    # Configure and return argparser object for reading command line arguments
    argparser_obj = argparse.ArgumentParser(prog="merge_technical_replicates")

    # Path to sample sheet excel file
    argparser_obj.add_argument("--sample-sheet",
                               action="store",
                               type=cli.file_type_arg,
                               dest="sample_sheet_file",
                               required=True,
                               help="Path to sample sheet to merge")

    # Batch name to be associated with all workflows
    argparser_obj.add_argument("--sample-id-col",
                               action="store",
                               type=str,
                               dest="sample_id_col",
                               required=True,
                               help="Name of sample id column")

    # Output prefix
    argparser_obj.add_argument("--output-file",
                               action="store",
                               type=str,
                               dest="output_file",
                               required=True,
                               help="Output file")

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
    ss_file  = args.sample_sheet_file
    id_col = args.sample_id_col
    output_file = args.output_file

    # Configure logging appropriate for verbosity
    utils.configure_logging(args.verbosity_level)

    # Read in sample sheet from excel file
    sample_sheet = pd.read_excel(ss_file)
    col_order = sample_sheet.columns

    if id_col not in sample_sheet.columns:
        err_msg = f"Id column '{id_col}' missing from sample sheet!"
        logging.error(err_msg)
        raise IOError(err_msg)

    # Coerce to list of records
    ss_inputs = sample_sheet.to_dict(orient="records")

    # Create new dict where each sample only has 1 entry
    merged_samples = {}
    for ss_input in ss_inputs:
        sample = ss_input[id_col]

        # Add new sample record if it doesn't already exist
        if sample not in merged_samples:
            merged_samples[sample] = ss_input
            continue


        # Otherwise append all the non-id columns to the existing values with commas
        for colname, val in ss_input.items():
            if colname != id_col:
                merged_samples[sample][colname] += f", {val}"

    # Coerce down to a flat list instead of a dict
    merged_dataset = [value for key, value in merged_samples.items()]

    # Coerce back to dataframe
    merged_dataset = pd.DataFrame.from_records(merged_dataset)

    logging.info(f"Detected {len(merged_dataset)} unique samples!")

    # Write to output file
    merged_dataset.to_excel(output_file, index=False)



if __name__ == "__main__":
    sys.exit(main())
