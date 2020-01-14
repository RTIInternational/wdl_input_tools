import logging


def validate_ss_against_wdl_template(sample_sheet, wdl_template):
    errors = False

    # Get the dataframe underneath the sample sheet object
    sample_sheet = sample_sheet.sample_sheet

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
