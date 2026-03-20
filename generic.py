import json
import os
import math
import boto3
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ----------------------------
# Environment configuration
# ----------------------------
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO = [x.strip() for x in os.getenv("EMAIL_TO", "").split(",") if x.strip()]

HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "14"))
BASELINE_PERIOD_SECONDS = int(os.getenv("BASELINE_PERIOD_SECONDS", "300"))
CURRENT_LOOKBACK_MINUTES = int(os.getenv("CURRENT_LOOKBACK_MINUTES", "10"))
DEFAULT_THRESHOLD = float(os.getenv("DEFAULT_THRESHOLD", "20"))
PERCENTILES = [int(x.strip()) for x in os.getenv("PERCENTILES", "95,99").split(",") if x.strip()]

SQS_QUEUE_THRESHOLD = float(os.getenv("SQS_QUEUE_THRESHOLD", "20"))
SQS_AGE_THRESHOLD_SECONDS = float(os.getenv("SQS_AGE_THRESHOLD_SECONDS", "600"))
SQS_RATE_WINDOW_MINUTES = int(os.getenv("SQS_RATE_WINDOW_MINUTES", "15"))

FAIL_OPEN = os.getenv("FAIL_OPEN", "true").lower() == "true"


# ----------------------------
# Utilities
# ----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_json(obj: Any) -> str:
    return json.dumps(obj, default=str)


def percentile(values: List[float], p: int) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])

    k = (len(s) - 1) * (p / 100.0)
    f = int(math.floor(k))
    c = min(f + 1, len(s) - 1)

    if f == c:
        return float(s[f])

    return float(s[f] + (s[c] - s[f]) * (k - f))


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _parse_time(t: Any) -> Optional[datetime]:
    if not t:
        return None
    if isinstance(t, datetime):
        return t.astimezone(timezone.utc)
    try:
        return datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def build_clients(region: Optional[str] = None):
    region_name = region or os.getenv("AWS_REGION")
    cw = boto3.client("cloudwatch", region_name=region_name)
    ses = boto3.client("sesv2", region_name=region_name)
    return cw, ses


def _dim_to_key(dimensions: List[Dict[str, str]]) -> str:
    if not dimensions:
        return ""
    return ",".join(sorted([f"{d['Name']}={d['Value']}" for d in dimensions if d.get("Name")]))


def _get_dimension_value(dimensions: List[Dict[str, str]], key: str) -> Optional[str]:
    for d in dimensions or []:
        if d.get("Name") == key:
            return d.get("Value")
    return None


# ----------------------------
# Alarm event parsing
# Supports:
# 1. EventBridge alarm events
# 2. Direct CloudWatch -> Lambda events
# 3. Your current test format with alarmData.configuration.metrics
# ----------------------------
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

    return {
        "metric": None,
        "threshold": None,
        "comparison_operator": None,
        "period": None,
        "statistic": None
    }


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
    """
    Supports your current test payload:
    {
      "alarmName": "...",
      "alarmData": {
        "configuration": {
          "metrics": [
            {
              "metricStat": {
                "metric": {
                  "namespace": "AWS/SQS",
                  "name": "ApproximateNumberOfMessagesVisible",
                  "dimensions": {
                    "QueueName": "queue-name"
                  }
                }
              }
            }
          ]
        },
        "state": {...}
      }
    }
    """
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
                "threshold": _safe_float(cfg.get("threshold")),
                "comparison_operator": cfg.get("comparisonOperator"),
                "period": ms.get("period") or BASELINE_PERIOD_SECONDS,
                "statistic": ms.get("stat") or "Average",
            }

    return {
        "metric": None,
        "threshold": None,
        "comparison_operator": None,
        "period": None,
        "statistic": None
    }


def parse_alarm_event(event: Dict[str, Any]) -> Dict[str, Any]:
    # EventBridge event format
    if event.get("detail-type") == "CloudWatch Alarm State Change" and "detail" in event:
        d = event["detail"]
        alarm_name = d.get("alarmName", "")
        state = (d.get("state", {}) or {}).get("value", "")
        region = event.get("region", os.getenv("AWS_REGION", ""))
        account = event.get("account", "")
        event_time = _parse_time(event.get("time")) or now_utc()

        metric_info = _extract_metric_from_eventbridge_detail(d)

        if not metric_info.get("metric"):
            raise ValueError("Unable to extract metric information from EventBridge alarm event payload.")

        return {
            "source": "eventbridge",
            "alarm_name": alarm_name,
            "state": state,
            "region": region,
            "account": account,
            "time": event_time,
            **metric_info,
        }

    # Direct CloudWatch alarm action format
    alarm_name = event.get("AlarmName") or event.get("alarmName") or ""
    state = event.get("NewStateValue") or event.get("state") or ""
    region = event.get("Region") or event.get("region") or os.getenv("AWS_REGION", "")
    account = event.get("AWSAccountId") or event.get("accountId") or event.get("account") or ""
    event_time = _parse_time(event.get("StateChangeTime")) or _parse_time(event.get("time")) or now_utc()

    metric_info = _extract_metric_from_direct_alarm(event)

    # Your current custom/test event format
    if not metric_info.get("metric") and "alarmData" in event:
        metric_info = _extract_metric_from_alarmdata(event)
        if not state:
            state = ((event.get("alarmData", {}) or {}).get("state", {}) or {}).get("value", "")

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


# ----------------------------
# CloudWatch metric retrieval
# ----------------------------
def get_metric_data_all(cw, namespace, metric_name, dimensions, stat, period, start, end):
    query = [{
        "Id": "m1",
        "MetricStat": {
            "Metric": {
                "Namespace": namespace,
                "MetricName": metric_name,
                "Dimensions": dimensions
            },
            "Period": period,
            "Stat": stat
        },
        "ReturnData": True
    }]

    timestamps = []
    values = []
    next_token = None

    while True:
        kwargs = {
            "MetricDataQueries": query,
            "StartTime": start,
            "EndTime": end,
            "ScanBy": "TimestampAscending",
        }
        if next_token:
            kwargs["NextToken"] = next_token

        resp = cw.get_metric_data(**kwargs)
        results = resp.get("MetricDataResults") or []
        if results:
            r0 = results[0]
            ts = r0.get("Timestamps") or []
            vs = r0.get("Values") or []
            for t, v in zip(ts, vs):
                timestamps.append(
                    t.astimezone(timezone.utc) if isinstance(t, datetime) else _parse_time(t) or now_utc()
                )
                values.append(float(v))

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return timestamps, values


def choose_current_value(ts, values, mode="latest"):
    if not values:
        return 0.0
    if not ts or len(ts) != len(values):
        return float(values[-1])

    if mode == "max":
        return float(max(values))
    if mode == "avg":
        return float(sum(values) / max(len(values), 1))

    idx = max(range(len(ts)), key=lambda i: ts[i])
    return float(values[idx])


# ----------------------------
# Analysis
# ----------------------------
def compute_baseline(history_values):
    if not history_values:
        return {"avg": 0.0, "max": 0.0, **{f"p{p}": 0.0 for p in PERCENTILES}}

    avg = float(sum(history_values) / len(history_values))
    mx = float(max(history_values))
    out = {"avg": avg, "max": mx}
    for p in PERCENTILES:
        out[f"p{p}"] = percentile(history_values, p)
    return out


def generic_verdict(current, baseline, threshold):
    reasons = []
    p95 = baseline.get("p95", 0.0)
    p99 = baseline.get("p99", 0.0)

    if current > max(threshold, p95):
        reasons.append(f"current {current:.2f} is above max(threshold={threshold:.2f}, p95={p95:.2f})")

    if p99 and current > p99:
        reasons.append(f"current {current:.2f} is above p99={p99:.2f}")

    if reasons:
        return "UNUSUAL SPIKE - MANUAL REVIEW REQUIRED", reasons

    return "LIKELY NORMAL BURST", ["current pattern is within threshold and historical percentiles"]


def sqs_enrichment(cw, queue_name, alarm_time):
    start_cur = alarm_time - timedelta(minutes=CURRENT_LOOKBACK_MINUTES)
    end_cur = alarm_time + timedelta(minutes=1)

    dims = [{"Name": "QueueName", "Value": queue_name}]

    ts_age, v_age = get_metric_data_all(
        cw,
        "AWS/SQS",
        "ApproximateAgeOfOldestMessage",
        dims,
        "Average",
        BASELINE_PERIOD_SECONDS,
        start_cur,
        end_cur
    )
    current_age = choose_current_value(ts_age, v_age, mode="latest")

    start_rate = alarm_time - timedelta(minutes=SQS_RATE_WINDOW_MINUTES)
    end_rate = alarm_time + timedelta(minutes=1)

    _, v_sent = get_metric_data_all(
        cw,
        "AWS/SQS",
        "NumberOfMessagesSent",
        dims,
        "Sum",
        BASELINE_PERIOD_SECONDS,
        start_rate,
        end_rate
    )
    _, v_del = get_metric_data_all(
        cw,
        "AWS/SQS",
        "NumberOfMessagesDeleted",
        dims,
        "Sum",
        BASELINE_PERIOD_SECONDS,
        start_rate,
        end_rate
    )

    return {
        "current_age_seconds": current_age,
        "sent_window": SQS_RATE_WINDOW_MINUTES,
        "messages_sent": float(sum(v_sent)) if v_sent else 0.0,
        "messages_deleted": float(sum(v_del)) if v_del else 0.0,
    }


def sqs_verdict(current_visible, baseline_visible, sqs_extra):
    reasons = []
    p95 = baseline_visible.get("p95", 0.0)

    if current_visible > max(SQS_QUEUE_THRESHOLD, p95):
        reasons.append(
            f"visible backlog {current_visible:.2f} > max(queue_threshold={SQS_QUEUE_THRESHOLD:.2f}, p95={p95:.2f})"
        )

    age = float(sqs_extra.get("current_age_seconds", 0.0))
    if age >= SQS_AGE_THRESHOLD_SECONDS:
        reasons.append(
            f"oldest message age {age:.0f}s >= age_threshold={SQS_AGE_THRESHOLD_SECONDS:.0f}s"
        )

    sent = float(sqs_extra.get("messages_sent", 0.0))
    deleted = float(sqs_extra.get("messages_deleted", 0.0))
    if sent > deleted:
        reasons.append(
            f"producer rate > consumer rate over last {SQS_RATE_WINDOW_MINUTES}m (sent={sent:.0f}, deleted={deleted:.0f})"
        )

    if reasons:
        return "UNUSUAL SPIKE - MANUAL REVIEW REQUIRED", reasons

    return "LIKELY NORMAL BURST", ["SQS backlog/age/rate signals look within normal operating patterns"]


# ----------------------------
# Email
# ----------------------------
def send_email(ses, subject: str, message: str):
    if not EMAIL_FROM or not EMAIL_TO:
        logger.info("EMAIL_FROM or EMAIL_TO not set; skipping email")
        logger.info("Subject=%s", subject)
        logger.info("Message=\n%s", message)
        return

    ses.send_email(
        FromEmailAddress=EMAIL_FROM,
        Destination={"ToAddresses": EMAIL_TO},
        Content={
            "Simple": {
                "Subject": {"Data": subject[:200]},
                "Body": {
                    "Text": {"Data": message}
                }
            }
        }
    )


# ----------------------------
# Message builder
# ----------------------------
def build_message(parsed, baseline, current, threshold, verdict, reasons, extra):
    metric = parsed["metric"]
    namespace = metric["namespace"]
    metric_name = metric["name"]
    dims = metric.get("dimensions", [])

    lines = []
    lines.append("CloudWatch Alarm Historical Baseline Analysis")
    lines.append("")
    lines.append(f"Alarm Name: {parsed.get('alarm_name')}")
    lines.append(f"Source: {parsed.get('source')}")
    lines.append(f"State: {parsed.get('state')}")
    lines.append(f"Alarm Time (UTC): {parsed.get('time')}")
    lines.append("")
    lines.append(f"Metric: {namespace}/{metric_name}")
    dim_str = ", ".join([f"{d['Name']}={d['Value']}" for d in dims]) if dims else "(none)"
    lines.append(f"Dimensions: {dim_str}")
    lines.append("")
    lines.append(f"Current Value: {current:.2f}")
    lines.append(f"Configured Threshold: {threshold:.2f}")
    lines.append("")
    lines.append(f"Historical Window: Last {HISTORY_DAYS} days")
    lines.append(f"Baseline Avg: {baseline.get('avg', 0.0):.2f}")
    lines.append(f"Baseline Max: {baseline.get('max', 0.0):.2f}")
    for p in PERCENTILES:
        lines.append(f"Baseline P{p}: {baseline.get(f'p{p}', 0.0):.2f}")

    if extra:
        lines.append("")
        lines.append("SQS Enrichment")
        lines.append(f"Oldest Message Age: {float(extra.get('current_age_seconds', 0.0)):.0f} seconds")
        lines.append(f"Rate Window: Last {int(extra.get('sent_window', SQS_RATE_WINDOW_MINUTES))} minutes")
        lines.append(f"Messages Sent: {float(extra.get('messages_sent', 0.0)):.0f}")
        lines.append(f"Messages Deleted: {float(extra.get('messages_deleted', 0.0)):.0f}")

    lines.append("")
    lines.append("Verdict:")
    lines.append(verdict)
    lines.append("")
    lines.append("Reason(s):")
    for r in reasons:
        lines.append(f"- {r}")

    return "\n".join(lines)


# ----------------------------
# Lambda handler
# ----------------------------
def lambda_handler(event, context):
    logger.info("Received event: %s", safe_json(event))

    try:
        parsed = parse_alarm_event(event)
        region = parsed.get("region") or os.getenv("AWS_REGION")
        cw, ses = build_clients(region)

        alarm_name = parsed["alarm_name"]
        state = parsed["state"]
        alarm_time = parsed["time"]

        metric = parsed["metric"]
        namespace = metric["namespace"]
        metric_name = metric["name"]
        dimensions = metric.get("dimensions", [])
        dim_key = _dim_to_key(dimensions)

        if state and state.upper() != "ALARM":
            logger.info("Alarm state is %s; skipping analysis.", state)
            return {"statusCode": 200, "body": json.dumps({"skipped": True, "state": state})}

        start_hist = alarm_time - timedelta(days=HISTORY_DAYS)
        end_hist = alarm_time

        stat = parsed.get("statistic") or "Average"
        period = int(parsed.get("period") or BASELINE_PERIOD_SECONDS)

        ts_hist, v_hist = get_metric_data_all(
            cw,
            namespace,
            metric_name,
            dimensions,
            stat,
            period,
            start_hist,
            end_hist
        )
        if not v_hist:
            raise RuntimeError(
                f"No historical CloudWatch data for {namespace}/{metric_name} dims={dim_key} (last {HISTORY_DAYS} days)"
            )

        baseline = compute_baseline(v_hist)

        start_cur = alarm_time - timedelta(minutes=CURRENT_LOOKBACK_MINUTES)
        end_cur = alarm_time + timedelta(minutes=1)

        ts_cur, v_cur = get_metric_data_all(
            cw,
            namespace,
            metric_name,
            dimensions,
            stat,
            period,
            start_cur,
            end_cur
        )

        current_mode = "latest"
        if namespace == "AWS/SQS" and metric_name == "ApproximateNumberOfMessagesVisible":
            current_mode = "max"

        current_value = choose_current_value(ts_cur, v_cur, mode=current_mode) if v_cur else float(v_hist[-1])

        threshold = parsed.get("threshold")
        threshold_value = float(threshold) if threshold is not None else float(DEFAULT_THRESHOLD)

        if namespace == "AWS/SQS":
            queue_name = _get_dimension_value(dimensions, "QueueName")
            if queue_name and metric_name == "ApproximateNumberOfMessagesVisible":
                extra = sqs_enrichment(cw, queue_name, alarm_time)
                verdict, reasons = sqs_verdict(current_value, baseline, extra)
            else:
                extra = {}
                verdict, reasons = generic_verdict(current_value, baseline, threshold_value)
        else:
            extra = {}
            verdict, reasons = generic_verdict(current_value, baseline, threshold_value)

        msg = build_message(
            parsed=parsed,
            baseline=baseline,
            current=current_value,
            threshold=threshold_value,
            verdict=verdict,
            reasons=reasons,
            extra=extra
        )

        subject = f"Alarm baseline analysis: {alarm_name} -> {verdict}"
        send_email(ses, subject, msg)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "alarm_name": alarm_name,
                "state": state,
                "metric": f"{namespace}/{metric_name}",
                "current": current_value,
                "baseline": baseline,
                "threshold": threshold_value,
                "verdict": verdict,
                "reasons": reasons,
            }, default=str),
        }

    except Exception as e:
        logger.exception("Analysis failed: %s", str(e))

        if FAIL_OPEN:
            try:
                region = (event.get("region") or event.get("Region") or os.getenv("AWS_REGION"))
                _, ses = build_clients(region)
                send_email(
                    ses,
                    "Alarm baseline analysis FAILED",
                    f"Analysis failed.\n\nError: {str(e)}\n\nEvent:\n{safe_json(event)}"
                )
            except Exception:
                logger.exception("Failed to send failure email.")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "error": str(e),
                    "fail_open": True
                })
            }

        raise
