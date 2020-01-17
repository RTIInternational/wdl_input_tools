import argparse
import os

import wdl_input_tools.cromwell as cromwell


def prefix_type_arg(arg_string):
    if arg_string.endswith("/"):
        err_msg = "File prefixes cannot be a directory! Make sure you're file prefix doesn't end with '/'!"
        raise argparse.ArgumentTypeError(err_msg)
    return arg_string


def file_type_arg(arg_string):
    if not os.path.exists(arg_string):
        err_msg = "%s does not exist! " \
                  "Please provide a valid file!" % arg_string
        raise argparse.ArgumentTypeError(err_msg)

    return arg_string


def excel_type_arg(arg_string):
    if not arg_string.endswith(".xlsx"):
        err_msg = "Excel file must end with .xlsx"
        raise argparse.ArgumentTypeError(err_msg)
    return arg_string


def batch_type_arg(arg_string):
    if not cromwell.is_valid_label(arg_string):
        err_msg = "Cromwell labels can only contain alphanumeric characters, hyphens (-), and underscores(_)"
        raise argparse.ArgumentTypeError(err_msg)
    return arg_string

