import uuid

import boto3

from TavantTraining.swf.swf_config import SWFConfig

swf = boto3.client("swf")

def start_work_flow():
    swf.start_workflow_execution(
        domain = SWFConfig.DOMAIN,
        workflowId = str(uuid.uuid4()),
        workflowType = {
            "name":SWFConfig.WORKFLOW,
            "version": SWFConfig.WORKFLOW_VERSION
        },
        taskList = {"name":SWFConfig.TASK_LIST_NAME},
        executionStartToCloseTimeout='100',
        taskStartToCloseTimeout='100',
        childPolicy='TERMINATE'
    )