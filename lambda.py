import json
import os
import boto3

sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    alarm_name = event.get("alarmName", "UnknownAlarm")
    state = event.get("alarmData", {}).get("state", {}).get("value", "UNKNOWN")
    reason = event.get("alarmData", {}).get("state", {}).get("reason", "No reason provided")

    subject = f"SQS Alert: {alarm_name} is {state}"
    message = f"""
CloudWatch Alarm Triggered

Alarm Name: {alarm_name}
State: {state}
Reason: {reason}

Full Event:
{json.dumps(event, indent=2)}
""".strip()

    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Notification sent",
            "sns_message_id": response["MessageId"]
        })
    }
