Response:
{
  "statusCode": 200,
  "body": "{\"error\": \"name 'parse_alarm_event' is not defined\", \"fail_open\": true}"
}

The area below shows the last 4 KB of the execution log.

Function Logs:
START RequestId: c288cdfa-8f26-4862-b65d-47643b586172 Version: $LATEST
[INFO]	2026-03-20T17:05:28.472Z	c288cdfa-8f26-4862-b65d-47643b586172	Received event: {"alarmName": "sqs_visible", "alarmData": {"configuration": {"metrics": [{"metricStat": {"metric": {"namespace": "AWS/SQS", "name": "ApproximateNumberOfMessagesVisible", "dimensions": {"QueueName": "sqs-alerting-poc-queue"}}}}]}, "state": {"value": "ALARM", "reason": "Threshold Crossed"}}}
[ERROR]	2026-03-20T17:05:28.473Z	c288cdfa-8f26-4862-b65d-47643b586172	Analysis failed: name 'parse_alarm_event' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 384, in lambda_handler
    parsed = parse_alarm_event(event)
             ^^^^^^^^^^^^^^^^^
NameError: name 'parse_alarm_event' is not defined
[INFO]	2026-03-20T17:05:28.631Z	c288cdfa-8f26-4862-b65d-47643b586172	Found credentials in environment variables.
[ERROR]	2026-03-20T17:05:30.703Z	c288cdfa-8f26-4862-b65d-47643b586172	Failed to send failure email.
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 384, in lambda_handler
    parsed = parse_alarm_event(event)
             ^^^^^^^^^^^^^^^^^
NameError: name 'parse_alarm_event' is not defined

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 479, in lambda_handler
    send_email(
    ~~~~~~~~~~^
        ses,
        ^^^^
        "Alarm baseline analysis FAILED",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        f"Analysis failed.\n\nError: {str(e)}\n\nEvent:\n{safe_json(event)}"
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "/var/task/lambda_function.py", line 320, in send_email
    ses.send_email(
    ~~~~~~~~~~~~~~^
        FromEmailAddress=EMAIL_FROM,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<8 lines>...
        }
        ^
    )
    ^
  File "/var/lang/lib/python3.14/site-packages/botocore/client.py", line 602, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^
  File "/var/lang/lib/python3.14/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
  File "/var/lang/lib/python3.14/site-packages/botocore/client.py", line 1078, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the SendEmail operation: User `arn:aws:sts::693779145154:assumed-role/sqs-alert-topic/sqsAlarmToSns' is not authorized to perform `ses:SendEmail' on resource `arn:aws:ses:us-east-2:693779145154:identity/mohammadghouse.shaik@fiserv.com'
END RequestId: c288cdfa-8f26-4862-b65d-47643b586172
REPORT RequestId: c288cdfa-8f26-4862-b65d-47643b586172	Duration: 2317.31 ms	Billed Duration: 2633 ms	Memory Size: 128 MB	Max Memory Used: 91 MB	Init Duration: 315.56 ms

Request ID: c288cdfa-8f26-4862-b65d-47643b586172




def parse_alarm_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if event.get("detail-type") == "CloudWatch Alarm State Change" and "detail" in event:
        d = event["detail"]
        alarm_name = d.get("alarmName", "")
        state = (d.get("state", {}) or {}).get("value", "")
        region = event.get("region", os.getenv("AWS_REGION", ""))
        account = event.get("account", "")
        event_time = _parse_time(event.get("time")) or now_utc()

        metric_info = _extract_metric_from_eventbridge_detail(d)
        return {
            "source": "eventbridge",
            "alarm_name": alarm_name,
            "state": state,
            "region": region,
            "account": account,
            "time": event_time,
            **metric_info,
        }

    alarm_name = event.get("AlarmName") or event.get("alarmName") or ""
    state = event.get("NewStateValue") or event.get("state") or ""
    region = event.get("Region") or event.get("region") or os.getenv("AWS_REGION", "")
    account = event.get("AWSAccountId") or event.get("accountId") or event.get("account") or ""
    event_time = _parse_time(event.get("StateChangeTime")) or _parse_time(event.get("time")) or now_utc()

    metric_info = _extract_metric_from_direct_alarm(event)

    # Support your current test event format:
    if not metric_info.get("metric") and "alarmData" in event:
        metric_info = _extract_metric_from_alarmdata(event)

    if not metric_info.get("metric"):
        raise ValueError("Unable to extract metric information from alarm event payload.")

    return {
        "source": "direct",
        "alarm_name": alarm_name,
        "state": state,
        "region": region,
        "account": account,
        "time": event_time,
        **metric_info,
    }


def _parse_time(t: Any) -> Optional[datetime]:
    if not t:
        return None
    if isinstance(t, datetime):
        return t.astimezone(timezone.utc)
    try:
        return datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _extract_metric_from_eventbridge_detail(detail: Dict[str, Any]) -> Dict[str, Any]:
    cfg = detail.get("configuration", {}) or {}
    metrics = cfg.get("metrics", []) or []

    for m in metrics:
        ms = m.get("metricStat", {}) or {}
        metric = ms.get("metric", {}) or {}
        namespace = metric.get("namespace")
        name = metric.get("name")
        dims = metric.get("dimensions", {}) or {}

        if namespace and name:
            dimensions = [{"Name": k, "Value": v} for k, v in dims.items()] if isinstance(dims, dict) else []
            return {
                "metric": {"namespace": namespace, "name": name, "dimensions": dimensions},
                "threshold": _safe_float(cfg.get("threshold")),
                "comparison_operator": cfg.get("comparisonOperator"),
                "period": ms.get("period"),
                "statistic": ms.get("stat"),
            }

    return {"metric": None, "threshold": None, "comparison_operator": None, "period": None, "statistic": None}


def _extract_metric_from_direct_alarm(event: Dict[str, Any]) -> Dict[str, Any]:
    trigger = event.get("Trigger", {}) or {}
    namespace = trigger.get("Namespace")
    name = trigger.get("MetricName")
    dims = trigger.get("Dimensions", []) or []

    dimensions = []
    for d in dims:
        n = d.get("name") or d.get("Name")
        v = d.get("value") or d.get("Value")
        if n and v is not None:
            dimensions.append({"Name": n, "Value": v})

    metric = {"namespace": namespace, "name": name, "dimensions": dimensions} if namespace and name else None

    return {
        "metric": metric,
        "threshold": _safe_float(trigger.get("Threshold")),
        "comparison_operator": trigger.get("ComparisonOperator"),
        "period": trigger.get("Period"),
        "statistic": trigger.get("Statistic"),
    }


def _extract_metric_from_alarmdata(event: Dict[str, Any]) -> Dict[str, Any]:
    alarm_data = event.get("alarmData", {}) or {}
    cfg = alarm_data.get("configuration", {}) or {}
    metrics = cfg.get("metrics", []) or []

    for m in metrics:
        ms = m.get("metricStat", {}) or {}
        metric = ms.get("metric", {}) or {}
        namespace = metric.get("namespace")
        name = metric.get("name")
        dims = metric.get("dimensions", {}) or {}

        if namespace and name:
            dimensions = [{"Name": k, "Value": v} for k, v in dims.items()] if isinstance(dims, dict) else []
            return {
                "metric": {"namespace": namespace, "name": name, "dimensions": dimensions},
                "threshold": None,
                "comparison_operator": None,
                "period": ms.get("period") or BASELINE_PERIOD_SECONDS,
                "statistic": ms.get("stat") or "Average",
            }

    return {"metric": None, "threshold": None, "comparison_operator": None, "period": None, "statistic": None}


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None
