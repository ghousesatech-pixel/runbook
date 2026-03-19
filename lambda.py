import json
import os
import boto3
import statistics
from datetime import datetime, timedelta, timezone

cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
HISTORY_DAYS = int(os.environ.get("HISTORY_DAYS", "14"))
QUEUE_THRESHOLD = int(os.environ.get("QUEUE_THRESHOLD", "20"))
AGE_THRESHOLD_SECONDS = int(os.environ.get("AGE_THRESHOLD_SECONDS", "600"))

def percentile(values, p):
    if not values:
        return 0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)

def get_metric_stats(queue_name, metric_name, statistic="Average", period=300, days=14):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/SQS",
                        "MetricName": metric_name,
                        "Dimensions": [
                            {"Name": "QueueName", "Value": queue_name}
                        ]
                    },
                    "Period": period,
                    "Stat": statistic
                },
                "ReturnData": True
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending"
    )

    results = response.get("MetricDataResults", [])
    if not results:
        return []

    values = results[0].get("Values", [])
    return values

def get_metric_sum_last_minutes(queue_name, metric_name, minutes=15):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes)

    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/SQS",
                        "MetricName": metric_name,
                        "Dimensions": [
                            {"Name": "QueueName", "Value": queue_name}
                        ]
                    },
                    "Period": 300,
                    "Stat": "Sum"
                },
                "ReturnData": True
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending"
    )

    results = response.get("MetricDataResults", [])
    if not results:
        return 0

    values = results[0].get("Values", [])
    return int(sum(values)) if values else 0

def extract_queue_name(event):
    # CloudWatch alarm event usually includes queue metric dimensions
    alarm_data = event.get("alarmData", {})
    configuration = alarm_data.get("configuration", {})
    metrics = configuration.get("metrics", [])

    for item in metrics:
        metric_stat = item.get("metricStat", {})
        metric = metric_stat.get("metric", {})
        if metric.get("namespace") == "AWS/SQS":
            dims = metric.get("dimensions", {})
            if isinstance(dims, dict) and "QueueName" in dims:
                return dims["QueueName"]

    # fallback: parse from alarm name if you use predictable naming
    alarm_name = event.get("alarmName", "")
    if alarm_name.startswith("sqs-") and "-depth" in alarm_name:
        temp = alarm_name.replace("sqs-", "")
        temp = temp.replace("-depth-high", "")
        temp = temp.replace("-depth", "")
        return temp

    raise ValueError("QueueName could not be extracted from the CloudWatch alarm event.")

def classify_spike(current_visible, p95_visible, current_age, sent_15m, deleted_15m):
    reasons = []

    if current_visible > max(QUEUE_THRESHOLD, p95_visible):
        reasons.append(
            f"current visible backlog {current_visible} is above historical p95 {round(p95_visible, 2)}"
        )

    if current_age >= AGE_THRESHOLD_SECONDS:
        reasons.append(
            f"oldest message age {current_age} seconds exceeds threshold {AGE_THRESHOLD_SECONDS}"
        )

    if sent_15m > deleted_15m:
        reasons.append(
            f"messages sent in last 15m ({sent_15m}) are greater than deleted ({deleted_15m})"
        )

    if reasons:
        return "UNUSUAL SPIKE - MANUAL REVIEW REQUIRED", reasons

    return "LIKELY NORMAL BURST", ["current pattern does not exceed configured historical/rate thresholds"]

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    queue_name = extract_queue_name(event)

    visible_history = get_metric_stats(
        queue_name=queue_name,
        metric_name="ApproximateNumberOfMessagesVisible",
        statistic="Average",
        period=300,
        days=HISTORY_DAYS
    )

    age_history = get_metric_stats(
        queue_name=queue_name,
        metric_name="ApproximateAgeOfOldestMessage",
        statistic="Average",
        period=300,
        days=HISTORY_DAYS
    )

    if not visible_history:
        raise Exception(f"No CloudWatch history returned for queue {queue_name}")

    current_visible = round(visible_history[-1], 2)
    current_age = round(age_history[-1], 2) if age_history else 0

    avg_visible = round(statistics.mean(visible_history), 2)
    max_visible = round(max(visible_history), 2)
    p95_visible = round(percentile(visible_history, 95), 2)

    sent_15m = get_metric_sum_last_minutes(queue_name, "NumberOfMessagesSent", minutes=15)
    deleted_15m = get_metric_sum_last_minutes(queue_name, "NumberOfMessagesDeleted", minutes=15)

    verdict, reasons = classify_spike(
        current_visible=current_visible,
        p95_visible=p95_visible,
        current_age=current_age,
        sent_15m=sent_15m,
        deleted_15m=deleted_15m
    )

    subject = f"SQS historical analysis: {queue_name} -> {verdict}"

    message = f"""
SQS Historical Analysis Alert

Queue Name: {queue_name}
Current Visible Messages: {current_visible}
Current Oldest Message Age: {current_age} seconds

Historical Window: Last {HISTORY_DAYS} days
Average Visible Messages: {avg_visible}
Maximum Visible Messages: {max_visible}
P95 Visible Messages: {p95_visible}

Last 15 Minutes
Messages Sent: {sent_15m}
Messages Deleted: {deleted_15m}

Verdict:
{verdict}

Reason(s):
- """ + "\n- ".join(reasons)

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject max is 100 chars
        Message=message
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "queue_name": queue_name,
            "current_visible": current_visible,
            "p95_visible": p95_visible,
            "verdict": verdict
        })
    }




{
  "alarmName": "sqs-queue-depth-20",
  "alarmData": {
    "state": {
      "value": "ALARM",
      "reason": "Threshold Crossed: 1 out of the last 1 datapoints was greater than or equal to the threshold (20)."
    }
  }
}
