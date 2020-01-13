import json
import pandas as pd
import logging
import os
import copy
import uuid


def validate_sample_sheet(wdl_template, sample_sheet):
    errors = False

    if not len(sample_sheet):
        logging.error("Samplesheet must contain 1 or more samples")
        errors = True

    for col in wdl_template.required_cols:
        # Check to make sure required columns are in sample sheet
        if col not in sample_sheet.columns:
            logging.error("Samplesheet missing required column: '{0}'".format(col))
            errors = True

        # Check to make sure all required columns have a value for every sample
        elif len(sample_sheet[sample_sheet[col].isnull()]) != 0:
            logging.error("One or more samples missing required value in required column: '{0}'".format(col))
            errors = True

    # Check to make sure sample sheet doesn't contain extraneous columns
    for col in sample_sheet.columns:
        if col not in wdl_template.cols:
            logging.error("Samplesheet contains column not found in WDL template: {0}".format(col))
            errors = True

    if errors:
        raise IOError("Samplesheet contained one or more errors. See above logs for details.")


class InputMaker:
    def __init__(self, sample_sheet_file, json_input_file):
        self.sample_sheet_file = sample_sheet_file
        self.json_input_file = json_input_file


class WDLInputTemplate:
    def __init__(self, wdl_input_file):

        # Check exists and Parse WDL input JSON
        if not os.path.exists(wdl_input_file):
            err_msg = "WDL input file does not exist: {0}! Please check paths".format(wdl_input_file)
            logging.error(err_msg)
            raise IOError(err_msg)

        with open(wdl_input_file, "r") as fh:
            self.wdl_input = json.load(fh)
        logging.info("Successfully parsed WDL from file {0}".format(wdl_input_file))

        # Get workflow name
        self.workflow_name = self.get_wf_name()
        logging.info("Guessed workflow name: {0}".format(self.workflow_name))

        # Get list of columns that can be set in sample sheet
        self.cols = [key for key in self.wdl_input if key.startswith(self.workflow_name)]

        # Get required columns that need to be input from sample sheet
        self.required_cols = [key for key, val in self.wdl_input.items() if key.startswith(self.workflow_name) and val == ""]
        logging.info("Columns that must be specified in sample sheet:\n{0}".format(", ".join(self.required_cols)))

        if not self.required_cols:
            err_msg = "WDL template JSON must have at least one empty value that needs to be filled by sample sheet!"
            logging.error(err_msg)
            raise IOError(err_msg)

    def get_wf_name(self):
        # Return name of workflow guess from WDL input JSON
        for key in self.wdl_input:
            if not key.startswith("#"):
                return key.split(".")[0]

    def create_sample_sheet_template_file(self, output_file, include_optional=False, num_samples=1):
        # Create an empty excel spreadsheet for user to fill in values to input to workflow
        cols_2_include = self.cols if include_optional else self.required_cols
        data = {k: [v]*num_samples for k,v in self.wdl_input.items() if k in cols_2_include}
        df = pd.DataFrame(data)
        df.to_excel(output_file, index=False)

    def get_template(self):
        return copy.deepcopy(self.wdl_input)


class SampleSheet:
    def __init__(self, wdl_template_file, sample_sheet_file, sample_id_col):
        self.wdl_template = WDLInputTemplate(wdl_template_file)
        self.sample_sheet = pd.read_excel(sample_sheet_file)
        self.sample_id_col = sample_id_col
        self.validate()

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

    def write_batch_wdl_input_json(self, output_file):
        output_jsons = []
        # Create a new template from each sample and plug in values from samplesheet
        for i in range(len(self.sample_sheet)):
            output_json = self.wdl_template.get_template()
            for col in self.sample_sheet.columns:
                output_json[col] = self.sample_sheet[col][i]
            output_jsons.append(output_json)

        # Write to file
        with open(output_file, "w") as fh:
            json.dump(output_jsons, fh, indent=1)

    def write_batch_wdl_label_json(self, output_file, batch_id, sample_id_col):
        output_jsons = []

        # Create a set of unique labels for each sample
        for i in range(len(self.sample_sheet)):
            sample_name = self.sample_sheet[sample_id_col][i]
            output_json = {"cromwell_batch_label": batch_id,
                           "cromwell_sample_run_label": "{0}_{1}_{2}_{3}".format(self.wdl_template.workflow_name,
                                                                                 batch_id,
                                                                                 sample_name,
                                                                                 str(uuid.uuid1()))[0:7]}
            output_jsons.append(output_json)

        # Write to file
        with open(output_file, "w") as fh:
            json.dump(output_jsons, fh, indent=1)



input_temp = "/Users/awaldrop/PycharmProjects/rnaseq-pipeline/test/rnaseq_pe_wf/test_rnaseq_pe_template_wf.json"
ss_file = "/Users/awaldrop/Desktop/rnaseq_template_req.xlsx"
#wdl_temp = WDLInputTemplate(input_temp)

#wdl_temp.create_sample_sheet_template_file("/Users/awaldrop/Desktop/rnaseq_template_req.xlsx", num_samples=10)
#wdl_temp.create_sample_sheet_template_file("/Users/awaldrop/Desktop/rnaseq_template.xlsx", include_optional=True, num_samples=10)

ss = SampleSheet(input_temp, ss_file)


