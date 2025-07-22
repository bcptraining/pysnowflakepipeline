import streamlit as st
import json
import os

st.set_page_config(page_title="Pipeline Dashboard", layout="wide")
st.title("🛠️ Pipeline Dashboard")

summary_path = "dashboard/pipeline_summary.json"

if os.path.exists(summary_path):
    with open(summary_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    st.subheader("📦 Load Summary")
    st.metric("Files Loaded", data["files_loaded"])
    st.metric("Rows Inserted", data["rows_inserted"])
    st.metric("Rows Rejected", data["rows_rejected"])
    st.text(f"Query ID: {data['query_id']}")

    st.subheader("⏱️ Timing")
    st.text(f"Start: {data['start_time']}")
    st.text(f"End:   {data['end_time']}")
    st.text(f"Duration: {data['duration_sec']} sec")

    if "rejects" in data:
        st.subheader("🚫 Reject Sample")
        st.write(f"Total rejects: {data['rejects']['count']}")
        st.write("Columns:", data['rejects']['columns'])

        if data["rejects"]["sample"]:
            st.write("Sample Rows:")
            st.dataframe(data["rejects"]["sample"])
        else:
            st.success("No reject sample rows available.")
else:
    st.warning("No pipeline summary found yet. Run the pipeline first!")
