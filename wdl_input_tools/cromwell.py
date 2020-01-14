import requests
import logging

from cromwell_tools.cromwell_api import CromwellAPI
from cromwell_tools.cromwell_auth import CromwellAuth

from wdl_input_tools import contants as const


class WFStatusCheckFailException(BaseException):
    pass


def get_cromwell_auth(url):
    # Provides cromwell authentication to be consumed by all API functions
    # Right now we only implement default authorization (no auth other than server URL)
    # Can expand upon this later but for now it really doesn't matter
    return CromwellAuth.harmonize_credentials(url=url)


def validate_cromwell_server(cromwell_auth):
    # Ping the cromwell server and check system health. Raise error if any issues returned by server.
    logging.info("Checking health of cromwell server...")
    result = CromwellAPI.health(cromwell_auth)
    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.error("Cromwell server is reachable but not functional! "
                      "Message from server:\n{0}".format(result.json()))
        raise
    logging.info("Cromwell server is up and running!")


def is_unique_batch_name(cromwell_auth, batch_name):
    # Return true if batch name has never been used on previous workflows, false otherwise
    query  = {"label": {const.CROMWELL_BATCH_LABEL: batch_name}}
    batch_wfs = query_workflows(cromwell_auth, query)
    return len(batch_wfs) == 0


def get_batch_conflicts(cromwell_auth, batch_sample_labels):
    # Query cromwell server to see if any active workflows currently exist in batch with same sample name
    # Return dictionary of conflicting workflow ids
    conflicting_wfs = {}
    for bs_label in batch_sample_labels:
        # Query only for active workflows in batch that have same batch_sample label
        query = {"label": {const.CROMWELL_BATCH_SAMPLE_LABEL: bs_label,
                           const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG}}
        wf_ids = query_workflows(cromwell_auth, query)
        if wf_ids:
            # Add conflicting workflow to hash
            logging.warning("Batch-Sample label already exists in batch: {0}".format(bs_label))
            conflicting_wfs[bs_label] = wf_ids
    return conflicting_wfs


def wf_exists(cromwell_auth, wf_id):
    # Return true if workflow exists, false otherwise
    try:
        get_wf_status(cromwell_auth, wf_id, log_on_fail=False)
        return True
    except WFStatusCheckFailException:
        return False


def get_wf_status(cromwell_auth, wf_id, log_on_fail=True):
    result = CromwellAPI.status(wf_id, cromwell_auth)
    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as e:
        err_msg = "Message from cromwell server: {0}".format(result.json()["message"])
        if log_on_fail:
            logging.error(err_msg)
        raise WFStatusCheckFailException(err_msg)
    return result.json()["status"]


def query_workflows(cromwell_auth, query):
    # Return worklfow ids matching conditions specified in query dict
    # e.g. query = {"label": [{"run_id": "12"},{"custom_label2": "barf"}]}
    # e.g. query = {"submission": "2020-01-10T14:53:48.128Z"}
    result = CromwellAPI.query(query, cromwell_auth)
    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error("Unable to run query: {0}".format(query))
        logging.error("Message from cromwell server:\n{0}".format(result.json()))
        raise
    return [wf["id"] for wf in result.json()['results']]


def get_wf_metadata(cromwell_auth, wf_id, include_keys=None, exclude_keys=None):
    result = CromwellAPI.metadata(wf_id,
                                  cromwell_auth,
                                  includeKey=include_keys,
                                  excludeKey=exclude_keys)

    try:
        result.raise_for_status()

    except requests.exceptions.HTTPError as e:
        logging.error("Unable to fetch metadata for wf: {0}".format(wf_id))
        logging.error("Message from cromwell server:\n{0}".format(result.json()))
        raise

    return result.json()


def update_wf_batch_status(cromwell_auth, wf_id, include_in_batch=True):
    # Update workflow batch status to indicate whether wf should be included in final batch or not
    labels = {const.CROMWELL_BATCH_STATUS_FIELD: const.CROMWELL_BATCH_STATUS_INCLUDE_FLAG}
    if not include_in_batch:
        labels[const.CROMWELL_BATCH_STATUS_FIELD] = const.CROMWELL_BATCH_STATUS_EXCLUDE_FLAG
    CromwellAPI.patch_labels(wf_id, labels, cromwell_auth, raise_for_status=True)


def get_wf_summary(cromwell_auth, wf_id):
    # Get a basic summary of a workflow that can be plugged immediately into a summary file
    include_keys = [const.CROMWELL_END_FIELD,
                    const.CROMWELL_START_FIELD,
                    const.CROMWELL_SUBMIT_FIELD,
                    const.CROMWELL_STATUS_FIELD,
                    const.CROMWELL_LABEL_FIELD,
                    const.CROMWELL_WF_NAME_FIELD]

    # Labels that are expected to be associated with batch workflows
    valid_labels = const.REQUIRED_WF_LABELS + [const.CROMWELL_WF_ID_FIELD]

    exclude_keys = ["submittedFiles", "calls", "inputs", "imports", "outputs"]
    metadata = get_wf_metadata(cromwell_auth, wf_id, exclude_keys=exclude_keys)

    # Subset to include only metadata we're interested in
    metadata = {k:v for k,v in metadata.items() if k in include_keys}

    # Unpack label dictionary
    label_dict = metadata.pop(const.CROMWELL_LABEL_FIELD)
    for k, v in label_dict.items():
        if k in valid_labels:
            metadata[k] = v
    return metadata
