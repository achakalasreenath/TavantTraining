import boto3

sns = boto3.client("sns")
swf = boto3.client("swf")

def prompt_for_contact():
    print("enter emain address")
    result = input()
    print(result)
    print("An confirmation email has been sent to {}, please confirm your subscription")
    return result


def subscribe_to_topic(email):
    result = sns.subscribe(TopicArn="arn:aws:sns:ap-south-1:150761330509:aws_swf", Protocol="email", Endpoint=email)
    return result


def wait_for_confirmation(result):
    if result["SubscriptionArn"] == 'pending confirmation':
        return False
    return True


def publish_message():
    sns.publish(TopicArn="arn:aws:sns:ap-south-1:150761330509:aws_swf", Message="Congratulations for subscribing")


activities = {'get_contact_activity': prompt_for_contact,
              'subscribe_topic_activity':subscribe_to_topic,
              'wait_for_confirmation_activity':wait_for_confirmation,
              'send_result_activity':publish_message}

while True:
    task = swf.poll_for_activity_task(
        domain = "sree-first-domain",
        taskList = activities
    )
