import pandas as pd
import uuid
import logging
import json
import copy
from collections import OrderedDict
import os
import yaml

import wdl_input_tools.validate as validate
import wdl_input_tools.contants as const


class BatchConfig:
    # Class for reading and loading batch configuration options from a yaml config file

    REQUIRED_KEYS = ["input_template", "sample_id_col", "sample_sheet_validators"]
    BASE_VALIDATION_FUNC = "validate_ss_against_wdl_template"

    def __init__(self, batch_config_yaml):

        # Check exists and Parse batch config yaml file
        if not os.path.exists(batch_config_yaml):
            err_msg = "WDL input file does not exist: {0}! Please check paths".format(batch_config_yaml)
            logging.error(err_msg)
            raise IOError(err_msg)

        # Parse yaml config
        with open(batch_config_yaml, "r") as fh:
            self.batch_config = yaml.safe_load(fh)
            logging.info("Successfully parsed WDL from file {0}".format(batch_config_yaml))

        # Make sure required keys are present
        self.validate()

        # Parse the WDL template from the json input template field
        self.batch_wdl_template = WDLInputTemplate(self.batch_config["input_template"])

        # Load functions needed to validate batch sample sheets
        self.sample_sheet_validators = self.get_sample_sheet_validation_functions()

        # Get the name of the key that is to be used as the sample id in the WDL template
        self.sample_id_col = self.batch_config["sample_id_col"]

    def validate(self):
        errors = False
        for key in self.REQUIRED_KEYS:
            if key not in self.batch_config:
                logging.error("Workflow batch missing required key: {0}".format(key))
        if errors:
            err_msg = "One or more required keys missing from batch config! See errors above for details."
            logging.error(err_msg)
            raise IOError(err_msg)

    def get_sample_sheet_validation_functions(self):

        # Get list of functios to apply to sample sheet from config
        sample_sheet_validate_funcs = self.batch_config["sample_sheet_validators"]
        if not isinstance(sample_sheet_validate_funcs, list):
            sample_sheet_validate_funcs = [sample_sheet_validate_funcs]

        # Add base validator if not present in batch template
        if self.BASE_VALIDATION_FUNC not in sample_sheet_validate_funcs:
            sample_sheet_validate_funcs.append(self.BASE_VALIDATION_FUNC)
        logging.debug(("Sample sheet validator functions: {0}".format(sample_sheet_validate_funcs)))

        # Loop through and load validation functions into single list
        validate_funcs = []
        errors = False
        for func_name in sample_sheet_validate_funcs:
            if not hasattr(validate, func_name):
                logging.error("Unable to import validation function '{0}' from validate module! "
                              "Make sure this function actually exists!".format(func_name))
                errors = True
            else:
                validate_funcs.append(getattr(validate, func_name))
        if errors:
            err_msg = "One or more sample sheet validation functions specified in " \
                      "workflow batch config could not be loaded!"
            logging.error(err_msg)
            raise IOError(err_msg)
        return validate_funcs

    def validate_sample_sheet(self, sample_sheet):
        # Validate sample sheet by calling all of the validation function
        # specified in the batch_config on the sample sheet
        for validator in self.sample_sheet_validators:
            # Call validation function to make sure sample sheet conforms to some set of specifications
            validator(sample_sheet, self.batch_wdl_template)


class WDLInputTemplate:
    # Class for holding the WDL JSON input file that will be used as a template
    # to generate batch input JSONs from a sample sheet
    def __init__(self, wdl_input_template):

        self.wdl_input = json.loads(wdl_input_template, object_pairs_hook=OrderedDict)
        logging.info("Successfully parsed WDL input template!")

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
    # Class for representing a set of sample information that will be used as input to a
    # batch of WDL workflows
    def __init__(self, sample_sheet_file, sample_id_col):
        self.sample_sheet = pd.read_excel(sample_sheet_file)
        self.sample_id_col = sample_id_col
        self.validate()

    @property
    def sample_names(self):
        return self.sample_sheet[self.sample_id_col].tolist()

    def validate(self):
        errors = False
        if self.sample_id_col not in self.sample_sheet.columns:
            logging.error("SampleSheet id_col '{0}' doesn't actually appear in sample sheet!".format(self.sample_id_col))
            errors = True

        if not len(self.sample_sheet):
            logging.error("Samplesheet must contain 1 or more samples")
            errors = True

        if len(self.sample_sheet[self.sample_id_col].unique()) != len(self.sample_sheet):
            logging.error("Sample IDs are not unique!")
            errors = True

        if len(self.sample_sheet[self.sample_sheet[self.sample_id_col].isnull()]) != 0:
            logging.error("One or more sample IDs is null! Cannot have empty sample ids!")
            errors = True

        if errors:
            err_msg = "Invalid SampleSheet! See above errors for details!"
            logging.error(err_msg)
            raise IOError(err_msg)


class CromwellStatusSheet:
    REQUIRED_COLS = [const.CROMWELL_UNIQUE_LABEL,
                     const.CROMWELL_SAMPLE_LABEL,
                     const.CROMWELL_BATCH_SAMPLE_LABEL,
                     const.CROMWELL_BATCH_LABEL]

    def __init__(self, status_sheet_file):
        self.status_sheet = pd.read_excel(status_sheet_file)

        # Check to make sure required columns are present
        errors = False
        for required_col in self.REQUIRED_COLS:
            if required_col not in self.status_sheet.columns:
                logging.error("Status sheet missing required column: {0}".format(required_col))
                errors = True
        if errors:
            err_msg = "Invalid CromwellStatusSheet! Missing one or more required columns. See above for details."
            logging.error(err_msg)
            raise IOError(err_msg)


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

    data = {k: [v] * num_samples for k, v in wdl_template.wdl_input.items() if k in cols_2_include}
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False)


def make_batch_inputs(sample_sheet, wdl_template):
    # Create a runnable WDL JSON input file from a sample sheet and WDL template
    batch_inputs = []
    sample_sheet = sample_sheet.sample_sheet if isinstance(sample_sheet, InputSampleSheet) else sample_sheet

    # Create a new template from each sample and plug in values from samplesheet
    for i in range(len(sample_sheet)):
        batch_input = wdl_template.get_template()
        for col in sample_sheet.columns:
            batch_input[col] = sample_sheet[col][i]
        batch_inputs.append(batch_input)
    return batch_inputs


def make_batch_labels(sample_sheet, wdl_template, batch_id):
    # Create a runnable WDL JSON label file from a sample sheet and WDL template
    batch_labels = []

    # Create a set of unique labels for each sample
    for sample_name in sample_sheet.sample_names:
        batch_label = {const.CROMWELL_BATCH_LABEL: batch_id,
                       const.CROMWELL_SAMPLE_LABEL: sample_name,
                       const.CROMWELL_BATCH_SAMPLE_LABEL: "{0}_{1}".format(batch_id, sample_name),
                       const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG,
                       const.CROMWELL_UNIQUE_LABEL: "{0}_{1}_{2}_{3}".format(wdl_template.workflow_name,
                                                                             batch_id,
                                                                             sample_name,
                                                                             str(uuid.uuid1())[0:7])}
        batch_labels.append(batch_label)
    return batch_labels


def validate_wf_labels(wf_labels):
    # Raise error if any of the required labels are missing from wf label dict
    required_labels = [const.CROMWELL_UNIQUE_LABEL,
                       const.CROMWELL_SAMPLE_LABEL,
                       const.CROMWELL_BATCH_SAMPLE_LABEL,
                       const.CROMWELL_BATCH_LABEL,
                       const.CROMWELL_BATCH_STATUS_FIELD]

    # Loop through and check all required labels
    for req_label in required_labels:
        # Raise error if label not present in workflow label set
        if req_label not in required_labels:
            if req_label != const.CROMWELL_UNIQUE_LABEL:
                err_msg = "Workflow '{0}' missing required label: {1}".format(wf_labels[const.CROMWELL_UNIQUE_LABEL],
                                                                              req_label)
            else:
                err_msg = "One or more workflow label dicts missing required label: {0}".format(req_label)
            logging.error(err_msg)
            raise IOError(err_msg)
