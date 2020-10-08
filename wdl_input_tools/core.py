import pandas as pd
import logging
import json
import copy
from collections import OrderedDict
import os
import yaml

import wdl_input_tools.validate as validate
import wdl_input_tools.contants as const
import wdl_input_tools.helpers as utils


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

        # Workflow type (scatter or gather)
        # Specifies how sample sheet will be interpreted and output to json for WDL input
        self.wf_type = self.batch_config["wf_type"]

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
    BATCH_OUTPUT_TOKEN = "<<<IMPORT_WF_OUTPUT>>>"
    BATCH_INPUT_TOKEN = "<<<IMPORT_WF_INPUT>>>"
    BATCH_LABEL_TOKEN = "<<<IMPORT_WF_LABEL>>>"

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

        # Determine if any required columns must be coerced to arrays
        self.array_cols = [key for key,val in self.wdl_input.items() if key in self.required_cols and isinstance(val, list)]
        logging.info("Required columns that will be coerced to arrays:\n{0}".format(", ".join(self.array_cols)))

        # Optional cols
        self.optional_cols = [key for key in self.wdl_input if key.startswith(self.workflow_name) and key not in self.required_cols]

        # Special keys for when workflow inputs must come from a previous batch workflow
        # This is mainly used for merge/gather workflows to pass inputs from upstream workflows to the template
        self.batch_input_cols  = {k: self.get_batch_val(v) for k,v in self.wdl_input.items() if self.get_input_type(v) == "input"}
        self.batch_output_cols = {k: self.get_batch_val(v) for k,v in self.wdl_input.items() if self.get_input_type(v) == "output"}
        self.batch_label_cols  = {k: self.get_batch_val(v) for k,v in self.wdl_input.items() if self.get_input_type(v) == "label"}

        self.validate()

    @property
    def imports_from_previous_batch(self):
        return len(self.batch_label_cols) + len(self.batch_output_cols) + len(self.batch_input_cols) > 0

    @property
    def batch_import_keys(self):
        return [k for k in self.batch_label_cols] + [k for k in self.batch_output_cols] + [k for k in self.batch_input_cols]

    def validate(self):
        # Make sure there are required columns. Otherwise what's the point of a template?
        if not self.required_cols:
            err_msg = "WDL template JSON must have at least one empty value that needs to be filled by sample sheet!"
            logging.error(err_msg)
            raise IOError(err_msg)

        # Check to make sure all batch label values are valid batch labels
        errors = False
        for key, val in self.batch_label_cols.items():
            if val not in const.REQUIRED_WF_LABELS + [const.CROMWELL_WF_ID_FIELD]:
                logging.error("Invalid batch label value {0} in WDL template!".format(val))
                errors = True
        if errors:
            err_msg = "One or more batch labels specified in WDL template is not an actual batch label!" \
                      "\nLabel options: {0}".format(", ".join(const.REQUIRED_WF_LABELS))
            logging.error(err_msg)
            raise IOError(err_msg)

    def is_required_col(self, key, val):
        if not key.startswith(self.workflow_name):
            # Eliminate comments or things that don't start with workflow name
            return False
        elif isinstance(val, list) and not len(val):
            # Empty arrays are required values that must be coerced to arrays
            return True
        elif not isinstance(val, str):
            # Otherwise anything that's not a string is not a required value
            return False
        if val == "" or self.get_input_type(val) is not None:
            return True
        return False

    def get_input_type(self, val):
        if not isinstance(val, str):
            return None
        if val.startswith(self.BATCH_INPUT_TOKEN):
            return "input"
        elif val.startswith(self.BATCH_OUTPUT_TOKEN):
            return "output"
        elif val.startswith(self.BATCH_LABEL_TOKEN):
            return "label"
        else:
            return None

    def get_batch_val(self, val):
        return val.replace(self.BATCH_LABEL_TOKEN, "").replace(self.BATCH_OUTPUT_TOKEN, "").replace(self.BATCH_INPUT_TOKEN, "").strip()

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

    def make_wf_input(self, required_vals):
        input_dict = self.get_template()
        for col in required_vals:
            val = required_vals[col]
            # Coerce to list if workflow is expecting a list
            if col in self.array_cols:
                # Remove any brackets and quoting and slit out by comma into list
                val = val.strip("[").strip("]").replace('"', '').replace("'", "").split(",")
                # Remove any leading/trailing whitespace
                val = [x.strip() for x in val]
            input_dict[col] = val
        return input_dict

    def get_input_val(self, key):
        return self.wdl_input[key]


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

    return {k: [v] * num_samples for k, v in wdl_template.wdl_input.items() if k in cols_2_include}


def get_wf_labels(sample_name, batch_name, workflow_name):
    label = {const.CROMWELL_BATCH_LABEL: batch_name,
             const.CROMWELL_SAMPLE_LABEL: sample_name,
             const.CROMWELL_BATCH_SAMPLE_LABEL: "{0}_{1}".format(batch_name, sample_name),
             const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG,
             const.CROMWELL_UNIQUE_LABEL: "{0}_{1}_{2}_{3}".format(workflow_name,
                                                                   batch_name,
                                                                   sample_name,
                                                                   utils.get_unique_id())}
    return label


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
