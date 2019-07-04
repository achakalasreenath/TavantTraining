import boto3

from TavantTraining.swf.swf_config import SWFConfig

sns = boto3.client("sns")
swf = boto3.client("swf")
topic_arn = "arn:aws:sns:ap-south-1:150761330509:aws-swf"
def prompt_for_contact(input):
    print("prompt_for_contact")
    return input


def subscribe_to_topic(email):
    print("subscribe to topic")
    result = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(result)
    print("An confirmation email has been sent to {}, please confirm your subscription")
    return email


def wait_for_confirmation(email):
    print("waiting for confirmation")
    is_confirmed = True

    while is_confirmed:
        subscriptions = [sub for sub in sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"] if
                         sub["Endpoint"] == email]
        print(subscriptions)
        if subscriptions[0]["SubscriptionArn"] != "PendingConfirmation":
            is_confirmed= False
    return "confirmed"

def publish_message(input):
    print("publishing message")
    sns.publish(TopicArn=topic_arn, Message="Congratulations for subscribing")
    return "Messgae sent"


activities = {'get_contact_4': prompt_for_contact,
              'subscribe_topic_4':subscribe_to_topic,
              'wait_for_confirmation_4':wait_for_confirmation,
              'send_result_4':publish_message}

while True:
    task = swf.poll_for_activity_task(
        domain = "sree-first-domain",
        taskList = {"name":SWFConfig.TASK_LIST_NAME}
    )
    print(task)
    if 'taskToken' not in task:
        print("task timeout")
    elif task["activityType"]["name"] in activities:
        input = task["input"]
        activity = task["activityType"]["name"]
        result = activities[activity](input)
        swf.respond_activity_task_completed(
            taskToken=task['taskToken'],
            result=result
        )
    else:
        print("activity not found")