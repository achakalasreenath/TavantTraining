import boto3
import uuid

from TavantTraining.swf.swf_config import SWFConfig

swf = boto3.client("swf")

while True:
    task = swf.poll_for_decision_task(
        domain=SWFConfig.DOMAIN,
        taskList={"name":SWFConfig.TASK_LIST_NAME}
    )

    if "taskToken" not in task:
        print("decision task timeout")
    else:
        eventHistory = [evt for evt in task["events"] if not evt['eventType'].startswith('Decision')]
        last_event = eventHistory[-1]
        if last_event["eventType"] == 'WorkflowExecutionStarted':
            swf.respond_decision_task_completed(
                taskToken=task["taskToken"],
                decisions=[
                    {
                        'decisionType': "ScheduleActivityTask",
                        'scheduleActivityTaskDecisionAttributes': {
                            'activityType': {
                                "name": SWFConfig.ACTIVITY_LIST[0]["name"],
                                "version":SWFConfig.ACTIVITY_LIST[0]["version"]
                            },
                            'activityId': str(uuid.uuid4()),
                            "input": "achakalasreenath1997@gmail.com"

                        }
                    }
                ]

            )


        elif last_event["eventType"] == 'ActivityTaskCompleted':
            activity_tasks_scheduled_history = [event for event in eventHistory if
                                                event["eventType"] == "ActivityTaskScheduled"]
            last_activity_scheduled = activity_tasks_scheduled_history[-1]
            if not last_activity_scheduled:
                swf.respond_decision_task_completed(
                    taskToken=task["taskToken"],
                    decisions=[
                        {
                            'decisionType': "WorkflowExecutionFailed",
                            'failWorkflowExecutionDecisionAttributes': {
                                'reason': 'last activity task scheduled cannot be found',
                                'details': 'string'
                            }

                        }

                    ]
                )
                continue

            activity = last_activity_scheduled["activityTaskScheduledEventAttributes"]["activityType"]["name"]
            input_param = last_event["activityTaskCompletedEventAttributes"]["result"]
            for i,act in enumerate(SWFConfig.ACTIVITY_LIST):
                 if act["name"] == activity:
                     if i != len(SWFConfig.ACTIVITY_LIST)-1:
                        next_activity = SWFConfig.ACTIVITY_LIST[i+1]
                     else:
                        next_activity = None

            if next_activity == None:
                 swf.respond_decision_task_completed(
                    taskToken=task["taskToken"],
                    decisions=[
                        {
                            'decisionType': "CompleteWorkflowExecution",
                            'completeWorkflowExecutionDecisionAttributes': {
                                'result': 'work flow completed',
                            }

                        }

                    ]
                )
            else:
                swf.respond_decision_task_completed(
                    taskToken=task["taskToken"],
                    decisions=[
                        {
                            'decisionType': "ScheduleActivityTask",
                            'scheduleActivityTaskDecisionAttributes': {
                                'activityType': {
                                "name": next_activity["name"],
                                "version": next_activity["version"]
                            },
                            'activityId': str(uuid.uuid4()),
                            "input": input_param
                            }

                        }

                    ]
                )
        elif last_event == "ActivityTaskTimedOut":
            swf.respond_decision_task_completed(
                taskToken=task["taskToken"],
                decisions=[
                    {
                        'decisionType': "FailWorkflowExecution",
                        'failWorkflowExecutionDecisionAttributes': {
                            'reason': 'task timeout',
                            'details': 'string'
                        }

                    }

                ]
            )
        elif last_event == "ActivityTaskFailed":
            swf.respond_decision_task_completed(
                taskToken=task["taskToken"],
                decisions=[
                    {
                        'decisionType': "FailWorkflowExecution",
                        'failWorkflowExecutionDecisionAttributes': {
                            'reason': 'task failed',
                            'details': 'string'
                        }

                    }

                ]
            )
        elif last_event["eventType"] == "WorkflowExecutionCompleted":
            print("Work Flow Execution Completed")
