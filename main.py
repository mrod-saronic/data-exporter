import os
import time
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from influxdb_client import InfluxDBClient
from datetime import timedelta
from tqdm import tqdm

# ----- UI -----
st.title("📊 InfluxDB Data Viewer")
st.markdown("Query and export InfluxDB data by multiple dates and assets")

# User inputs
asset_ids = st.multiselect("Select Asset IDs", options=["cr8", "cr9", "cr10", "cr11", "cr12", "cr13", "cr14", "cr15"], default=["cr8"])
measurement = st.selectbox("Measurement", options=["Odometry"])
field = st.selectbox("Field", options=["odometer"])
dates = st.multiselect("Select Dates", options=pd.date_range("2025-05-01", "2025-05-02"), default=[pd.Timestamp.today()])

if st.button("Run Query"):
    # ----- Connect to InfluxDB -----
    token = st.secrets["INFLUX_TOKEN"]
    org = st.secrets["INFLUX_ORG"]
    bucket = st.secrets["INFLUX_BUCKET"]
    url = st.secrets["INFLUX_HOST"]

    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    all_records = []

    for asset_id in asset_ids:
        for date in dates:
            start = pd.to_datetime(date).strftime("%Y-%m-%dT00:00:00Z")
            stop = (pd.to_datetime(date) + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

            flux_query = f'''
            from(bucket: "{bucket}")
              |> range(start: {start}, stop: {stop})
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> filter(fn: (r) => r.asset_id == "{asset_id}")
              |> filter(fn: (r) => r._field == "{field}")
              |> keep(columns: ["_value", "_time", "asset_id"])
              |> rename(columns: {{_value: "{field}", _time: "time_stamp"}})
            '''

            tables = query_api.query(flux_query)

            for table in tables:
                for record in table.records:
                    all_records.append({
                        "asset_id": asset_id,
                        field: record.get_value(),
                        "time_stamp": record.get_time()
                    })

        # ----- Process and Display -----
        if all_records:
            df = pd.DataFrame(all_records)
            df["time_stamp"] = pd.to_datetime(df["time_stamp"]).dt.tz_localize(None)
            st.success(f"{len(df)} records retrieved from {len(asset_ids)} asset(s) and {len(dates)} date(s)")

            # Chart: One line per asset
            st.subheader("📈 Chart")
            for asset_id in df["asset_id"].unique():
                df_asset = df[df["asset_id"] == asset_id]
                st.line_chart(df_asset.set_index("time_stamp")[field], height=300, use_container_width=True)

            # CSV Export
            st.subheader("📤 CSV Export (Per Asset)")
            filename_prefix = f"{field}_{date}"
            export_csv_data(df, field, filename_prefix)
            st.success("CSV files exported to your Downloads folder.")
        else:
            st.warning("No data found for the selected assets and dates.")


# Function to export as csv format
def export_csv_data(df, field, filename_prefix):
    base_path = "/Users/mariorodriguez/Downloads/"
    grouped = list(df.groupby("asset_id"))
    total = len(grouped)

    print(f"📄 Exporting {total} CSV file(s)...")

    start_time = time.time()
    progress = tqdm(total=total, desc="Exporting CSVs", unit="file")

    for i, (asset_id, group) in enumerate(grouped, start=1):
        filename = f"{filename_prefix}_{asset_id}.csv"
        csv_path = os.path.join(base_path, filename)
        group.to_csv(csv_path, index=False)

        progress.set_postfix_str(f"{asset_id}")
        progress.update(1)

    progress.close()
    duration = time.time() - start_time
    print(f"\n✅ All CSV files exported in {round(duration, 2)} seconds.")