import json
import os
import math
import boto3
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ----------------------------
# Environment configuration
# ----------------------------
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")  # Optional: if blank, just logs output
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "14"))
BASELINE_PERIOD_SECONDS = int(os.getenv("BASELINE_PERIOD_SECONDS", "300"))  # 5 min
CURRENT_LOOKBACK_MINUTES = int(os.getenv("CURRENT_LOOKBACK_MINUTES", "10"))  # query "current" window
DEFAULT_THRESHOLD = float(os.getenv("DEFAULT_THRESHOLD", "0"))  # used if alarm doesn't provide one
PERCENTILES = [int(x.strip()) for x in os.getenv("PERCENTILES", "95,99").split(",") if x.strip()]

# Optional SQS-specific thresholds (used only for SQS analysis)
SQS_QUEUE_THRESHOLD = float(os.getenv("SQS_QUEUE_THRESHOLD", "20"))  # visible messages
SQS_AGE_THRESHOLD_SECONDS = float(os.getenv("SQS_AGE_THRESHOLD_SECONDS", "600"))  # 10 minutes
SQS_RATE_WINDOW_MINUTES = int(os.getenv("SQS_RATE_WINDOW_MINUTES", "15"))

# Behavior tuning
FAIL_OPEN = os.getenv("FAIL_OPEN", "true").lower() == "true"
# FAIL_OPEN=True means: if analysis fails, function returns success after logging/publishing a failure message
# This prevents endless retries if CloudWatch keeps retrying async invoke. [1](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-actions-Lambda.html)


# ----------------------------
# Utilities
# ----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_json(obj: Any) -> str:
    return json.dumps(obj, default=str)


def percentile(values: List[float], p: int) -> float:
    """Linear interpolation percentile; safe for small lists."""
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


def build_clients(region: Optional[str] = None):
    # Use event region if available, otherwise default Lambda region
    region_name = region or os.getenv("AWS_REGION")
    cw = boto3.client("cloudwatch", region_name=region_name)
    sns = boto3.client("sns", region_name=region_name)
    return cw, sns


# ----------------------------
# Event parsing (supports both Direct Alarm -> Lambda and EventBridge)
# ----------------------------
def parse_alarm_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a normalized structure:
      {
        "source": "eventbridge" | "direct",
        "alarm_name": str,
        "state": str,
        "region": str,
        "account": str,
        "time": datetime,
        "metric": {
            "namespace": str,
            "name": str,
            "dimensions": [{"Name": "...", "Value": "..."}]
        },
        "threshold": float | None,
        "comparison_operator": str | None,
        "period": int | None,
        "statistic": str | None
      }
    """

    # EventBridge alarm state change event
    # detail-type: "CloudWatch Alarm State Change" [2](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch-and-eventbridge.html)
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

    # Direct CloudWatch alarm action -> Lambda integration [1](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-actions-Lambda.html)
    # Common keys: AlarmName, NewStateValue, Trigger
    alarm_name = event.get("AlarmName") or event.get("alarmName") or ""
    state = event.get("NewStateValue") or event.get("state") or ""
    region = event.get("Region") or event.get("region") or os.getenv("AWS_REGION", "")
    account = event.get("AWSAccountId") or event.get("accountId") or event.get("account") or ""
    event_time = _parse_time(event.get("StateChangeTime")) or _parse_time(event.get("time")) or now_utc()

    metric_info = _extract_metric_from_direct_alarm(event)
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
    # Most AWS events use ISO8601
    try:
        return datetime.fromisoformat(str(t).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _extract_metric_from_eventbridge_detail(detail: Dict[str, Any]) -> Dict[str, Any]:
    cfg = detail.get("configuration", {}) or {}
    metrics = cfg.get("metrics", []) or []

    # Best effort: find the first MetricStat metric (single metric alarms)
    # For metric math/composite alarms you'd need more logic; keeping generic for single-metric alarms.
    for m in metrics:
        ms = m.get("metricStat", {}) or {}
        metric = (ms.get("metric", {}) or {})
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

    # If we can't parse, return empty metric; caller will error
    return {"metric": None, "threshold": None, "comparison_operator": None, "period": None, "statistic": None}


def _extract_metric_from_direct_alarm(event: Dict[str, Any]) -> Dict[str, Any]:
    trigger = event.get("Trigger", {}) or {}
    namespace = trigger.get("Namespace")
    name = trigger.get("MetricName")
    dims = trigger.get("Dimensions", []) or []

    dimensions = []
    for d in dims:
        # Direct alarm dimensions often look like { "name": "...", "value": "..." }
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


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


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
# CloudWatch metric retrieval (paged)
# ----------------------------
def get_metric_data_all(
    cw,
    namespace: str,
    metric_name: str,
    dimensions: List[Dict[str, str]],
    stat: str,
    period: int,
    start: datetime,
    end: datetime,
) -> Tuple[List[datetime], List[float]]:
    """
    Retrieves all datapoints (timestamps + values) for a single metric query using GetMetricData with pagination.
    """
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

    timestamps: List[datetime] = []
    values: List[float] = []
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
        results = (resp.get("MetricDataResults") or [])
        if results:
            r0 = results[0]
            ts = r0.get("Timestamps") or []
            vs = r0.get("Values") or []
            # Timestamps/Values correspond by index
            for t, v in zip(ts, vs):
                # boto3 typically returns datetime for timestamps
                timestamps.append(t.astimezone(timezone.utc) if isinstance(t, datetime) else _parse_time(t) or now_utc())
                values.append(float(v))

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return timestamps, values


def choose_current_value(ts: List[datetime], values: List[float], mode: str = "latest") -> float:
    """
    mode:
      - latest: take the most recent datapoint by timestamp
      - max: take max over window
      - avg: average over window
    """
    if not values:
        return 0.0
    if not ts or len(ts) != len(values):
        # fallback
        return float(values[-1])

    if mode == "max":
        return float(max(values))
    if mode == "avg":
        return float(sum(values) / max(len(values), 1))

    # latest by timestamp
    idx = max(range(len(ts)), key=lambda i: ts[i])
    return float(values[idx])


# ----------------------------
# Analysis
# ----------------------------
def compute_baseline(history_values: List[float]) -> Dict[str, float]:
    if not history_values:
        return {"avg": 0.0, "max": 0.0, **{f"p{p}": 0.0 for p in PERCENTILES}}

    avg = float(sum(history_values) / len(history_values))
    mx = float(max(history_values))
    out = {"avg": avg, "max": mx}
    for p in PERCENTILES:
        out[f"p{p}"] = percentile(history_values, p)
    return out


def generic_verdict(
    current: float,
    baseline: Dict[str, float],
    threshold: float,
) -> Tuple[str, List[str]]:
    reasons = []
    p95 = baseline.get("p95", 0.0)
    p99 = baseline.get("p99", 0.0)

    # Compare against both absolute threshold and historical tail (p95/p99)
    if current > max(threshold, p95):
        reasons.append(f"current {current:.2f} is above max(threshold={threshold:.2f}, p95={p95:.2f})")

    if p99 and current > p99:
        reasons.append(f"current {current:.2f} is above p99={p99:.2f}")

    if reasons:
        return "UNUSUAL SPIKE - MANUAL REVIEW REQUIRED", reasons
    return "LIKELY NORMAL BURST", ["current pattern is within threshold and historical percentiles"]


def sqs_enrichment(
    cw,
    queue_name: str,
    region: str,
    alarm_time: datetime,
) -> Dict[str, Any]:
    """
    Adds SQS-specific signals:
      - ApproximateAgeOfOldestMessage (age)
      - NumberOfMessagesSent/Deleted over last N minutes (rate imbalance)
    SQS metrics are available in CloudWatch under AWS/SQS. [3](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html)
    """
    # Fetch current age over a small window
    start_cur = alarm_time - timedelta(minutes=CURRENT_LOOKBACK_MINUTES)
    end_cur = alarm_time + timedelta(minutes=1)

    dims = [{"Name": "QueueName", "Value": queue_name}]

    # Age (Average)
    ts_age, v_age = get_metric_data_all(
        cw,
        namespace="AWS/SQS",
        metric_name="ApproximateAgeOfOldestMessage",
        dimensions=dims,
        stat="Average",
        period=BASELINE_PERIOD_SECONDS,
        start=start_cur,
        end=end_cur,
    )
    current_age = choose_current_value(ts_age, v_age, mode="latest")

    # Rates (Sum)
    start_rate = alarm_time - timedelta(minutes=SQS_RATE_WINDOW_MINUTES)
    end_rate = alarm_time + timedelta(minutes=1)

    _, v_sent = get_metric_data_all(
        cw, "AWS/SQS", "NumberOfMessagesSent", dims, "Sum", BASELINE_PERIOD_SECONDS, start_rate, end_rate
    )
    _, v_del = get_metric_data_all(
        cw, "AWS/SQS", "NumberOfMessagesDeleted", dims, "Sum", BASELINE_PERIOD_SECONDS, start_rate, end_rate
    )
    sent = float(sum(v_sent)) if v_sent else 0.0
    deleted = float(sum(v_del)) if v_del else 0.0

    return {
        "current_age_seconds": current_age,
        "sent_window": SQS_RATE_WINDOW_MINUTES,
        "messages_sent": sent,
        "messages_deleted": deleted,
    }


def sqs_verdict(
    current_visible: float,
    baseline_visible: Dict[str, float],
    sqs_extra: Dict[str, Any],
) -> Tuple[str, List[str]]:
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
        reasons.append(f"producer rate > consumer rate over last {SQS_RATE_WINDOW_MINUTES}m (sent={sent:.0f}, deleted={deleted:.0f})")

    if reasons:
        return "UNUSUAL SPIKE - MANUAL REVIEW REQUIRED", reasons
    return "LIKELY NORMAL BURST", ["SQS backlog/age/rate signals look within normal operating patterns"]


# ----------------------------
# Notification
# ----------------------------
def publish_insight(sns, subject: str, message: str):
    if not SNS_TOPIC_ARN:
        logger.info("SNS_TOPIC_ARN not set; skipping publish. Subject=%s", subject)
        logger.info("Message:\n%s", message)
        return

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # SNS subject max 100 chars
        Message=message
    )


# ----------------------------
# Lambda handler
# ----------------------------
def lambda_handler(event, context):
    logger.info("Received event: %s", safe_json(event))

    try:
        parsed = parse_alarm_event(event)
        region = parsed.get("region") or os.getenv("AWS_REGION")
        cw, sns = build_clients(region)

        alarm_name = parsed["alarm_name"]
        state = parsed["state"]
        alarm_time: datetime = parsed["time"]

        metric = parsed["metric"]
        namespace = metric["namespace"]
        metric_name = metric["name"]
        dimensions = metric.get("dimensions", [])
        dim_key = _dim_to_key(dimensions)

        # Only analyze when ALARM (you can extend to OK as well)
        if state and state.upper() != "ALARM":
            logger.info("Alarm state is %s; skipping analysis.", state)
            return {"statusCode": 200, "body": json.dumps({"skipped": True, "state": state})}

        # Determine baseline window
        start_hist = alarm_time - timedelta(days=HISTORY_DAYS)
        end_hist = alarm_time

        # Use a sensible statistic default if missing
        stat = parsed.get("statistic") or "Average"
        period = int(parsed.get("period") or BASELINE_PERIOD_SECONDS)

        # Fetch historical baseline
        ts_hist, v_hist = get_metric_data_all(
            cw,
            namespace=namespace,
            metric_name=metric_name,
            dimensions=dimensions,
            stat=stat,
            period=period,
            start=start_hist,
            end=end_hist
        )
        if not v_hist:
            raise RuntimeError(f"No historical CloudWatch data for {namespace}/{metric_name} dims={dim_key} (last {HISTORY_DAYS} days)")

        baseline = compute_baseline(v_hist)

        # Fetch current value around alarm time
        start_cur = alarm_time - timedelta(minutes=CURRENT_LOOKBACK_MINUTES)
        end_cur = alarm_time + timedelta(minutes=1)
        ts_cur, v_cur = get_metric_data_all(
            cw,
            namespace=namespace,
            metric_name=metric_name,
            dimensions=dimensions,
            stat=stat,
            period=period,
            start=start_cur,
            end=end_cur
        )

        # "Current" selection: for backlog-like metrics, MAX is often more robust; keep generic as latest.
        # If this is SQS visible messages, use MAX to avoid missing short spikes.
        current_mode = "latest"
        if namespace == "AWS/SQS" and metric_name == "ApproximateNumberOfMessagesVisible":
            current_mode = "max"

        current_value = choose_current_value(ts_cur, v_cur, mode=current_mode) if v_cur else float(v_hist[-1])

        # Threshold: prefer alarm threshold; fallback to env default
        threshold = parsed.get("threshold")
        threshold_value = float(threshold) if threshold is not None else float(DEFAULT_THRESHOLD)

        # SQS-specific enrichment when possible
        verdict = None
        reasons: List[str] = []

        if namespace == "AWS/SQS":
            queue_name = _get_dimension_value(dimensions, "QueueName")
            if queue_name:
                extra = {}
                if metric_name == "ApproximateNumberOfMessagesVisible":
                    extra = sqs_enrichment(cw, queue_name, region, alarm_time)
                    verdict, reasons = sqs_verdict(current_value, baseline, extra)
                else:
                    # Generic for other SQS metrics
                    verdict, reasons = generic_verdict(current_value, baseline, threshold_value)
                    extra = sqs_enrichment(cw, queue_name, region, alarm_time)

                msg = build_message(
                    parsed=parsed,
                    baseline=baseline,
                    current=current_value,
                    threshold=threshold_value,
                    verdict=verdict,
                    reasons=reasons,
                    extra=extra
                )
            else:
                verdict, reasons = generic_verdict(current_value, baseline, threshold_value)
                msg = build_message(
                    parsed=parsed,
                    baseline=baseline,
                    current=current_value,
                    threshold=threshold_value,
                    verdict=verdict,
                    reasons=reasons,
                    extra={}
                )
        else:
            # Generic non-SQS alarm
            verdict, reasons = generic_verdict(current_value, baseline, threshold_value)
            msg = build_message(
                parsed=parsed,
                baseline=baseline,
                current=current_value,
                threshold=threshold_value,
                verdict=verdict,
                reasons=reasons,
                extra={}
            )

        subject = f"Alarm baseline analysis: {alarm_name} -> {verdict}"
        publish_insight(sns, subject, msg)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "alarm_name": alarm_name,
                "state": state,
                "metric": f"{namespace}/{metric_name}",
                "dimensions": dimensions,
                "current": current_value,
                "baseline": baseline,
                "threshold": threshold_value,
                "verdict": verdict,
                "reasons": reasons,
            }, default=str),
        }

    except Exception as e:
        logger.exception("Analysis failed: %s", str(e))

        # Production behavior: avoid infinite retries if CloudWatch is invoking asynchronously [1](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-actions-Lambda.html)
        if FAIL_OPEN:
            try:
                region = (event.get("region") or event.get("Region") or os.getenv("AWS_REGION"))
                _, sns = build_clients(region)
                subject = "Alarm baseline analysis FAILED"
                message = f"Analysis failed.\n\nError: {str(e)}\n\nEvent:\n{safe_json(event)}"
                publish_insight(sns, subject, message)
            except Exception:
                logger.exception("Failed to publish failure notification.")
            return {"statusCode": 200, "body": json.dumps({"error": str(e), "fail_open": True})}

        # FAIL_OPEN=False: raise so the invoker can retry
        raise


def build_message(
    parsed: Dict[str, Any],
    baseline: Dict[str, float],
    current: float,
    threshold: float,
    verdict: str,
    reasons: List[str],
    extra: Dict[str, Any],
) -> str:
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
    lines.append(f"Configured Threshold (alarm/env): {threshold:.2f}")
    lines.append("")
    lines.append(f"Historical Window: Last {HISTORY_DAYS} days")
    lines.append(f"Baseline Avg: {baseline.get('avg', 0.0):.2f}")
    lines.append(f"Baseline Max: {baseline.get('max', 0.0):.2f}")
    for p in PERCENTILES:
        lines.append(f"Baseline P{p}: {baseline.get(f'p{p}', 0.0):.2f}")

    if extra:
        lines.append("")
        lines.append("SQS Enrichment")
        if "current_age_seconds" in extra:
            lines.append(f"Oldest Message Age: {float(extra.get('current_age_seconds', 0.0)):.0f} seconds")
        if "sent_window" in extra:
            lines.append(f"Rate Window: Last {int(extra.get('sent_window', SQS_RATE_WINDOW_MINUTES))} minutes")
        if "messages_sent" in extra:
            lines.append(f"Messages Sent: {float(extra.get('messages_sent', 0.0)):.0f}")
        if "messages_deleted" in extra:
            lines.append(f"Messages Deleted: {float(extra.get('messages_deleted', 0.0)):.0f}")

    lines.append("")
    lines.append("Verdict:")
    lines.append(verdict)
    lines.append("")
    lines.append("Reason(s):")
    for r in reasons:
        lines.append(f"- {r}")

    return "\n".join(lines)
