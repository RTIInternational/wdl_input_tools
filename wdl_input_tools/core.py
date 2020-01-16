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

    REQUIRED_KEYS = ["input_template", "sample_id_col", "sample_sheet_validators", "wf_type"]
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

        if self.batch_config["wf_type"] not in ["scatter", "gather"]:
            logging.error("Batch config wf_type must be either 'scatter' or 'gather'!")
            errors = True

        if errors:
            err_msg = "One or more required keys missing or misspecified in batch config! See errors above for details."
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
    BATCH_OUTPUT_TOKEN = "@@@"
    BATCH_LABEL_TOKEN = "$$$"

    def __init__(self, wdl_input_template):

        self.wdl_input = json.loads(wdl_input_template, object_pairs_hook=OrderedDict)
        logging.info("Successfully parsed WDL input template!")

        # Get workflow name
        self.workflow_name = self.get_wf_name()
        logging.info("Guessed workflow name: {0}".format(self.workflow_name))

        # Get list of columns that can be set in sample sheet
        self.cols = [key for key in self.wdl_input if key.startswith(self.workflow_name)]

        # Get required columns that need to be input from sample sheet
        self.required_cols = [key for key, val in self.wdl_input.items() if self.is_required_col(key, val)]
        logging.info("Columns that must be specified in sample sheet:\n{0}".format(", ".join(self.required_cols)))

        # Optional cols
        self.optional_cols = [key for key in self.wdl_input if key.startswith(self.workflow_name) and key not in self.required_cols]

        # Special columns that point to inputs and labels from previous batch workflows
        # This is mainly used for merge/gather workflows to pass inputs from upstream workflows to the template
        self.batch_output_cols = {k: self.get_batch_val(v) for k,v in self.wdl_input.items() if self.is_batch_output_col(v)}
        self.batch_label_cols  = {k: self.get_batch_val(v) for k,v in self.wdl_input.items() if self.is_batch_label_col(v)}

        self.validate()

    @property
    def imports_from_batch(self):
        return len(self.batch_label_cols) + len(self.batch_output_cols) > 0

    @property
    def batch_import_keys(self):
        return [k for k in self.batch_label_cols] + [k for k in self.batch_output_cols]

    def validate(self):
        # Make sure there are required columns. Otherwise what's the point of a template?
        if not self.required_cols:
            err_msg = "WDL template JSON must have at least one empty value that needs to be filled by sample sheet!"
            logging.error(err_msg)
            raise IOError(err_msg)

        # Check to make sure all batch label values are valid batch labels
        errors = False
        for key, val in self.batch_label_cols.items():
            if val not in const.REQUIRED_WF_LABELS:
                logging.error("Invalid batch label value {0} in WDL template!".format(val))
                errors = True
        if errors:
            err_msg = "One or more batch labels specified in WDL template is not an actual batch label!" \
                      "\nLabel options: {0}".format(", ".join(const.REQUIRED_WF_LABELS))
            logging.error(err_msg)
            raise IOError(err_msg)

    def is_required_col(self, key, val):
        if not key.startswith(self.workflow_name):
            return False
        elif not isinstance(val, str):
            return False
        if val == "" or self.is_batch_label_col(val) or self.is_batch_output_col(val):
            return True
        return False

    def is_batch_output_col(self, val):
        if not isinstance(val, str):
            return False
        return val.startswith(self.BATCH_OUTPUT_TOKEN)

    def is_batch_label_col(self, val):
        if not isinstance(val, str):
            return False
        return val.startswith(self.BATCH_LABEL_TOKEN)

    def get_batch_val(self, val):
        return val.replace(self.BATCH_LABEL_TOKEN, "").replace(self.BATCH_OUTPUT_TOKEN, "")

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


def init_sample_sheet(wdl_template, optional_cols=[], num_samples=1):
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
    return df


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

    # Loop through and check all required labels
    for req_label in const.REQUIRED_WF_LABELS:
        # Raise error if label not present in workflow label set
        if req_label not in wf_labels:
            if req_label != const.CROMWELL_UNIQUE_LABEL:
                err_msg = "Workflow '{0}' missing required label: {1}".format(wf_labels[const.CROMWELL_UNIQUE_LABEL],
                                                                              req_label)
            else:
                err_msg = "One or more workflow label dicts missing required label: {0}".format(req_label)
            logging.error(err_msg)
            raise IOError(err_msg)
