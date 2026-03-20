Response:
{
  "statusCode": 200,
  "body": "{\"error\": \"An error occurred (MessageRejected) when calling the SendEmail operation: Email address is not verified. The following identities failed the check in region US-EAST-2: mohammadghouse.shaik@fiserv.com\", \"fail_open\": true}"
}

The area below shows the last 4 KB of the execution log.

Function Logs:
SendEmail operation: Email address is not verified. The following identities failed the check in region US-EAST-2: mohammadghouse.shaik@fiserv.com
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 612, in lambda_handler
    send_email(ses, subject, msg)
    ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "/var/task/lambda_function.py", line 455, in send_email
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
botocore.errorfactory.MessageRejected: An error occurred (MessageRejected) when calling the SendEmail operation: Email address is not verified. The following identities failed the check in region US-EAST-2: mohammadghouse.shaik@fiserv.com
[ERROR]	2026-03-20T18:26:40.257Z	711ea9fd-3a41-47ec-8706-752bdb62ae2f	Failed to send failure email.
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 612, in lambda_handler
    send_email(ses, subject, msg)
    ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "/var/task/lambda_function.py", line 455, in send_email
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
botocore.errorfactory.MessageRejected: An error occurred (MessageRejected) when calling the SendEmail operation: Email address is not verified. The following identities failed the check in region US-EAST-2: mohammadghouse.shaik@fiserv.com

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 635, in lambda_handler
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
  File "/var/task/lambda_function.py", line 455, in send_email
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
botocore.errorfactory.MessageRejected: An error occurred (MessageRejected) when calling the SendEmail operation: Email address is not verified. The following identities failed the check in region US-EAST-2: mohammadghouse.shaik@fiserv.com
END RequestId: 711ea9fd-3a41-47ec-8706-752bdb62ae2f
REPORT RequestId: 711ea9fd-3a41-47ec-8706-752bdb62ae2f	Duration: 3653.83 ms	Billed Duration: 3654 ms	Memory Size: 128 MB	Max Memory Used: 95 MB

Request ID: 711ea9fd-3a41-47ec-8706-752bdb62ae2f
