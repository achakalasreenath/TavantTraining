import boto3
from TavantTraining.swf.swf_config import SWFConfig

swf = boto3.client("swf")

def register_workflow_type():
    work_flow = swf.register_workflow_type(
        domain = SWFConfig.DOMAIN,
        name = SWFConfig.WORKFLOW,
        version = SWFConfig.WORKFLOW_VERSION,
        defaultTaskList = {"name":SWFConfig.TASK_LIST_NAME}
    )
    return work_flow

def register_domain():
    domain = swf.register_domain(
        name = SWFConfig.DOMAIN,
        workflowExecutionRetentionPeriodInDays = '1'
    )

def register_activities():
    for activity in SWFConfig.ACTIVITY_LIST:
        swf.register_activity_type(
            domain = SWFConfig.DOMAIN,
            name = activity['name'],
            version = activity["version"],
            defaultTaskStartToCloseTimeout='100',
            defaultTaskHeartbeatTimeout='100',
            defaultTaskList={
                "name":SWFConfig.TASK_LIST_NAME
            },
            defaultTaskScheduleToStartTimeout='100',
            defaultTaskScheduleToCloseTimeout='100'
        )