import boto3

from TavantTraining.swf.swf_config import SWFConfig

swf = boto3.client("swf")

def register_domain(domain_name,history_retention_period):
    domain = swf.register_domain(
        name = domain_name,
        workflowExecutionRetentionPeriodInDays = history_retention_period
    )
    return domain
def register_workflow_type(domain_name):
    work_flow = swf.register_workflow_type(
        domain = "sree-first-domain",
        name = 'work_flow_1',
        version = '1',
        defaultTaskList = SWFConfig.TASK_LIST_NAME
    )
    return work_flow

def poll_for_decision_tasks():
    swf.poll_for_decision_task(
        domain = "sree-first-domain",
        defaultTaskList={
            'name': 'get_contact_activity',
            'name': 'subscribe_topic_activity',
            'name': 'wait_for_confirmation_activity',
            'name': 'send_result_activity'
        }

    )