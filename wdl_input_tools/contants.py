CROMWELL_BATCH_LABEL = "cromwell-batch-label"
CROMWELL_SAMPLE_LABEL = "cromwell-sample-label"
CROMWELL_BATCH_SAMPLE_LABEL = "cromwell-batch-sample-label"
CROMWELL_UNIQUE_LABEL = "cromwell-unique-label"
CROMWELL_START_FIELD = "start"
CROMWELL_END_FIELD = "end"
CROMWELL_SUBMIT_FIELD = "submission"
CROMWELL_WF_ID_FIELD = "cromwell-workflow-id"
CROMWELL_STATUS_FIELD = "status"
CROMWELL_RUNNING_STATUS = "Running"
CROMWELL_FAILED_STATUS = "Failed"
CROMWELL_ABORTED_STATUS = "Aborted"
CROMWELL_ABORTING_STATUS = "Aborting"
CROMWELL_SUCCESS_STATUS = "Succeeded"
CROMWELL_SUBMITTED_STATUS = "Submitted"
CROMWELL_LABEL_FIELD = "labels"
CROMWELL_BATCH_STATUS_FIELD = "cromwell-batch-status"
CROMWELL_BATCH_STATUS_INCLUDE_FLAG = "include"
CROMWELL_BATCH_STATUS_EXCLUDE_FLAG = "exclude"
CROMWELL_WF_NAME_FIELD = "workflowName"
SUPERCEDED_WF_FIELD = "SupercededWFs"
REPORT_INFO_FIELD = "Info"

REQUIRED_WF_LABELS = [CROMWELL_UNIQUE_LABEL,
                      CROMWELL_BATCH_STATUS_FIELD,
                      CROMWELL_BATCH_LABEL,
                      CROMWELL_BATCH_SAMPLE_LABEL,
                      CROMWELL_SAMPLE_LABEL]

REPORT_COL_ORDER = [CROMWELL_WF_ID_FIELD,
                    CROMWELL_UNIQUE_LABEL,
                    CROMWELL_SAMPLE_LABEL,
                    CROMWELL_BATCH_LABEL,
                    CROMWELL_BATCH_SAMPLE_LABEL,
                    CROMWELL_BATCH_STATUS_FIELD,
                    SUPERCEDED_WF_FIELD,
                    REPORT_INFO_FIELD]

STATUS_COL_ORDER = [CROMWELL_WF_ID_FIELD,
                    CROMWELL_BATCH_STATUS_FIELD,
                    CROMWELL_SAMPLE_LABEL,
                    CROMWELL_UNIQUE_LABEL,
                    CROMWELL_BATCH_LABEL,
                    CROMWELL_BATCH_SAMPLE_LABEL,
                    CROMWELL_STATUS_FIELD,
                    CROMWELL_SUBMIT_FIELD,
                    CROMWELL_START_FIELD,
                    CROMWELL_END_FIELD,
                    CROMWELL_WF_NAME_FIELD]

