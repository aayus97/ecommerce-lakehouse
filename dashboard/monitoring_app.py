from pathlib import Path
import json

import pandas as pd
import streamlit as st

from src.config import load_app_config, path_value

config = load_app_config()
METRICS_DIR = Path(path_value(config, "metrics"))

SYSTEM_METRICS = {
    "pipeline_steps",
    "pipeline_runs",
    "orders_data_quality",
}


def load_metrics():
    records = []

    for file in sorted(METRICS_DIR.glob("*.jsonl")):
        with file.open("r") as f:
            for line_number, line in enumerate(f, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    st.warning(f"Skipped invalid JSON in {file.name}:{line_number}")
                    continue

                record["source_file"] = file.name
                records.append(record)

    df = pd.DataFrame(records)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    return df


def status_badge(status):
    normalized = str(status).lower()
    if normalized == "success":
        return "success"
    if normalized == "failed":
        return "failed"
    return normalized or "unknown"


def highlight_status(row):
    status = str(row.get("status", "")).lower()
    if status == "failed":
        return ["background-color: #fee2e2; color: #991b1b"] * len(row)
    if status == "success":
        return ["background-color: #dcfce7; color: #166534"] * len(row)
    return [""] * len(row)


st.set_page_config(
    page_title="Lakehouse Pipeline Monitoring",
    layout="wide",
)

st.title("Lakehouse Pipeline Monitoring")

if not METRICS_DIR.exists():
    st.warning("No metrics directory found. Run the pipeline first.")
    st.stop()

df = load_metrics()

if df.empty:
    # st.warning("No metrics records found in metrics/*.jsonl.")
    st.warning(f"No metrics records found in {METRICS_DIR}/*.jsonl.")

    st.stop()

st.sidebar.header("Filters")

metric_names = (
    sorted(df["metric_name"].dropna().unique()) if "metric_name" in df.columns else []
)
selected_metrics = st.sidebar.multiselect(
    "Metric types",
    metric_names,
    default=metric_names,
)

filtered_df = df.copy()

if selected_metrics:
    filtered_df = filtered_df[filtered_df["metric_name"].isin(selected_metrics)]

if "timestamp" in filtered_df.columns and filtered_df["timestamp"].notna().any():
    min_date = filtered_df["timestamp"].min().date()
    max_date = filtered_df["timestamp"].max().date()
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
        start_ts = pd.Timestamp(start_date, tz="UTC")
        end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
        filtered_df = filtered_df[
            (filtered_df["timestamp"] >= start_ts) & (filtered_df["timestamp"] < end_ts)
        ]

if "run_id" not in filtered_df.columns:
    st.error("No run_id column found. Run the pipeline with PIPELINE_RUN_ID metrics.")
    st.stop()

run_ids = sorted(filtered_df["run_id"].dropna().unique(), reverse=True)

if not run_ids:
    st.warning("No pipeline runs match the selected filters.")
    st.stop()

selected_run_id = st.sidebar.selectbox("Pipeline run", run_ids, index=0)
run_df = filtered_df[filtered_df["run_id"] == selected_run_id].copy()

pipeline_run_df = run_df[run_df["metric_name"] == "pipeline_runs"].copy()
steps_df = run_df[run_df["metric_name"] == "pipeline_steps"].copy()
quality_df = run_df[run_df["metric_name"] == "orders_data_quality"].copy()
failed_records = run_df[
    run_df.get("status", pd.Series(dtype=str)).astype(str).str.lower() == "failed"
]

run_status = "unknown"
if not pipeline_run_df.empty and "status" in pipeline_run_df.columns:
    run_status = status_badge(pipeline_run_df["status"].dropna().iloc[-1])
elif not failed_records.empty:
    run_status = "failed"

if run_status == "failed":
    st.error("This pipeline run has failures.")
elif run_status == "success":
    st.success("This pipeline run completed successfully.")
else:
    st.info("This pipeline run does not have a final status yet.")

st.subheader("Selected Pipeline Run")

pipeline_name = (
    run_df["pipeline_name"].dropna().iloc[0]
    if "pipeline_name" in run_df.columns and not run_df["pipeline_name"].dropna().empty
    else "Unknown"
)

if not pipeline_run_df.empty and "duration_seconds" in pipeline_run_df.columns:
    total_duration = pipeline_run_df["duration_seconds"].dropna().iloc[-1]
elif "duration_seconds" in run_df.columns:
    total_duration = run_df["duration_seconds"].dropna().sum()
else:
    total_duration = 0

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Pipeline", pipeline_name)

with col2:
    st.metric("Run ID", selected_run_id)

with col3:
    st.metric("Run Status", run_status)

with col4:
    st.metric("Duration Seconds", round(float(total_duration), 2))

if not pipeline_run_df.empty:
    summary_cols = [
        "total_steps",
        "successful_steps",
        "failed_steps",
        "skipped_steps",
        "failed_step",
        "skipped_steps_list",
    ]
    available_summary_cols = [c for c in summary_cols if c in pipeline_run_df.columns]

    if available_summary_cols:
        st.subheader("Run Summary")
        st.dataframe(
            pipeline_run_df[available_summary_cols],
            use_container_width=True,
            hide_index=True,
        )

st.subheader("Pipeline Steps")

if steps_df.empty:
    st.info("No pipeline step metrics found for this run.")
else:
    columns_to_show = [
        "timestamp",
        "step",
        "module",
        "status",
        "attempt",
        "max_attempts",
        "duration_seconds",
        "return_code",
    ]
    available_columns = [c for c in columns_to_show if c in steps_df.columns]
    steps_view = steps_df[available_columns].sort_values("timestamp")

    st.dataframe(
        steps_view.style.apply(highlight_status, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Step Status Counts")
        st.bar_chart(steps_df["status"].value_counts())

    with col2:
        st.subheader("Step Duration")
        duration_df = steps_df[["step", "duration_seconds"]].dropna()
        if not duration_df.empty:
            st.bar_chart(duration_df.set_index("step"))

st.subheader("Orders Data Quality")

if quality_df.empty:
    st.info("No orders data quality metrics found for this run.")
else:
    latest_quality = quality_df.sort_values("timestamp").iloc[-1]
    total_rows = int(latest_quality.get("total_rows", 0))
    valid_rows = int(latest_quality.get("valid_rows", 0))
    invalid_rows = int(latest_quality.get("invalid_rows", 0))
    invalid_percentage = float(latest_quality.get("invalid_percentage", 0))

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Rows", total_rows)

    with col2:
        st.metric("Valid Rows", valid_rows)

    with col3:
        st.metric("Invalid Rows", invalid_rows)

    with col4:
        st.metric("Invalid %", invalid_percentage)

    if invalid_percentage > 5:
        st.error("Data quality alert: invalid rows are above 5%.")
    elif invalid_percentage > 0:
        st.warning("Some invalid rows were found and quarantined.")
    else:
        st.success("No invalid rows found.")

    st.dataframe(quality_df.sort_values("timestamp"), use_container_width=True)

quality_history_df = filtered_df[
    filtered_df["metric_name"] == "orders_data_quality"
].copy()

if not quality_history_df.empty and "invalid_percentage" in quality_history_df.columns:
    st.subheader("Invalid Row Trend")
    trend_df = quality_history_df.dropna(subset=["timestamp", "invalid_percentage"])
    if not trend_df.empty:
        st.line_chart(
            trend_df.sort_values("timestamp").set_index("timestamp")[
                "invalid_percentage"
            ]
        )

st.subheader("Pipeline Run History")

runs_df = filtered_df[filtered_df["metric_name"] == "pipeline_runs"].copy()

if runs_df.empty:
    st.info("No pipeline run summary metrics found.")
else:
    columns_to_show = [
        "timestamp",
        "pipeline_name",
        "run_id",
        "status",
        "duration_seconds",
        "total_steps",
        "successful_steps",
        "failed_steps",
        "skipped_steps",
        "failed_step",
    ]
    available_columns = [c for c in columns_to_show if c in runs_df.columns]
    runs_view = runs_df[available_columns].sort_values("timestamp", ascending=False)

    st.dataframe(
        runs_view.style.apply(highlight_status, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    trend_df = runs_df.dropna(subset=["timestamp", "duration_seconds"])

    if not trend_df.empty:
        st.subheader("Pipeline Duration Trend")
        st.line_chart(
            trend_df.sort_values("timestamp").set_index("timestamp")["duration_seconds"]
        )

st.subheader("Business Metrics")

business_df = run_df[~run_df["metric_name"].isin(SYSTEM_METRICS)].copy()

if business_df.empty:
    st.info("No business or gold metrics found for this run.")
else:
    metric_options = sorted(business_df["metric_name"].dropna().unique())
    selected_business_metric = st.selectbox("Business metric", metric_options)
    selected_business_df = business_df[
        business_df["metric_name"] == selected_business_metric
    ]

    st.dataframe(
        selected_business_df.sort_values("timestamp"),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Failures")

if failed_records.empty:
    st.success("No failures found for this run.")
else:
    st.error("Failures found.")
    st.dataframe(failed_records, use_container_width=True, hide_index=True)
