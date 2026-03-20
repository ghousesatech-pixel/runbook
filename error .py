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
