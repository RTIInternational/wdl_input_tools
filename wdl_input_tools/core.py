import pandas as pd
import uuid
import logging
import os
import json
import copy
from collections import OrderedDict


class WDLInputTemplate:
    def __init__(self, wdl_input_file):

        # Check exists and Parse WDL input JSON
        if not os.path.exists(wdl_input_file):
            err_msg = "WDL input file does not exist: {0}! Please check paths".format(wdl_input_file)
            logging.error(err_msg)
            raise IOError(err_msg)

        with open(wdl_input_file, "r") as fh:
            self.wdl_input = json.load(fh, object_pairs_hook=OrderedDict)
        logging.info("Successfully parsed WDL from file {0}".format(wdl_input_file))

        # Get workflow name
        self.workflow_name = self.get_wf_name()
        logging.info("Guessed workflow name: {0}".format(self.workflow_name))

        # Get list of columns that can be set in sample sheet
        self.cols = [key for key in self.wdl_input if key.startswith(self.workflow_name)]

        # Get required columns that need to be input from sample sheet
        self.required_cols = [key for key, val in self.wdl_input.items() if key.startswith(self.workflow_name) and val == ""]
        logging.info("Columns that must be specified in sample sheet:\n{0}".format(", ".join(self.required_cols)))

        # Optional cols
        self.optional_cols = [key for key in self.wdl_input if key.startswith(self.workflow_name) and key not in self.required_cols]

        if not self.required_cols:
            err_msg = "WDL template JSON must have at least one empty value that needs to be filled by sample sheet!"
            logging.error(err_msg)
            raise IOError(err_msg)

    def get_wf_name(self):
        # Return name of workflow guess from WDL input JSON
        for key in self.wdl_input:
            if not key.startswith("#"):
                return key.split(".")[0]

    def get_template(self):
        return copy.deepcopy(self.wdl_input)

    def get_valid_key_name(self, key):
        key = "{0}.{1}".format(self.workflow_name, key) if not key.startswith(self.workflow_name) else key
        return key


class InputSampleSheet:
    def __init__(self, wdl_template_file, sample_sheet_file, sample_id_col):
        self.wdl_template = WDLInputTemplate(wdl_template_file)
        self.sample_sheet = pd.read_excel(sample_sheet_file)
        self.sample_id_col = sample_id_col
        self.validate()

    @property
    def sample_names(self):
        return self.sample_sheet[self.sample_id_col].tolist()

    def validate(self):
        errors = False

        if not len(self.sample_sheet):
            logging.error("Samplesheet must contain 1 or more samples")
            errors = True

        for col in self.wdl_template.required_cols:
            # Check to make sure required columns are in sample sheet
            if col not in self.sample_sheet.columns:
                logging.error("Samplesheet missing required column: '{0}'".format(col))
                errors = True

            # Check to make sure all required columns have a value for every sample
            elif len(self.sample_sheet[self.sample_sheet[col].isnull()]) != 0:
                logging.error("One or more samples missing required value in required column: '{0}'".format(col))
                errors = True

        # Check to make sure sample sheet doesn't contain extraneous columns
        for col in self.sample_sheet.columns + ["cromwell_batch_label", "cromwell_sample_run_label"]:
            if col not in self.wdl_template.cols:
                logging.error("Samplesheet contains column not found in WDL template: {0}".format(col))
                errors = True

        if len(self.sample_sheet[self.sample_id_col].unique()) != len(self.sample_sheet):
            logging.error("Sample IDs are not unique!")
            errors = True

        if errors:
            raise IOError("Samplesheet contained one or more errors. See above logs for details.")


def init_sample_sheet_file(wdl_template, output_file, optional_cols=[], num_samples=1):
    # Create an empty excel spreadsheet for user to fill in values to input to workflow
    cols_2_include = wdl_template.required_cols

    # Check and add optional columns
    invalid_cols = False
    for col in optional_cols:
        # Convert column to workflow namespace
        valid_col = wdl_template.get_valid_key_name(col)

        # Raise error if key not specified in WDL template
        if valid_col not in wdl_template.cols:
            logging.error("Optional column '{0}' does not appear in WDL JSON template".format(col))
            invalid_cols = True

        # Add column for output if it's not already there
        if valid_col not in cols_2_include:
            cols_2_include.append(valid_col)

    if invalid_cols:
        err_msg = "One or more optional columns passed to init_sample_sheet_file do not appear in WDL template! " \
                  "Modify WDL template to include column or remove from optional-cols!"

        logging.error(err_msg)
        raise IOError(err_msg)

    data = {k: [v ] * num_samples for k, v in wdl_template.wdl_input.items() if k in cols_2_include}
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False)


def make_batch_inputs(sample_sheet, wdl_template):
    batch_inputs = []
    # Create a new template from each sample and plug in values from samplesheet
    for i in range(len(sample_sheet)):
        batch_input = wdl_template.get_template()
        for col in sample_sheet.columns:
            batch_input[col] = sample_sheet[col][i]
        batch_inputs.append(batch_input)
    return batch_inputs


def make_batch_labels(sample_sheet, wdl_template, batch_id):
    batch_labels = []

    # Create a set of unique labels for each sample
    for sample_name in sample_sheet.sample_names:
        batch_label = {"cromwell_batch_label": batch_id,
                       "cromwell_sample_label": sample_name,
                       "cromwell_sample_run_label": "{0}_{1}_{2}_{3}".format(wdl_template.workflow_name,
                                                                             batch_id,
                                                                             sample_name,
                                                                             str(uuid.uuid1()))[0:7]}
        batch_labels.append(batch_label)
    return batch_labels
