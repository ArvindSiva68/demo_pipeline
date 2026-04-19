# app/pages/1_risk_evolution.py
import streamlit as st
import duckdb
import plotly.express as px

st.title("Auditable Entity Risk Evolution")

con = duckdb.connect("data/pipeline.duckdb", read_only=True)
df  = con.execute("SELECT * FROM gold_entity_risk_evolution").df()
con.close()

# Filters
col1, col2 = st.columns(2)
bu     = col1.multiselect("Business unit", df["business_unit"].unique())
region = col2.multiselect("Region",        df["region"].unique())

if bu:     df = df[df["business_unit"].isin(bu)]
if region: df = df[df["region"].isin(region)]

# KPI row
k1, k2, k3 = st.columns(3)
k1.metric("Entities monitored",  len(df))
k2.metric("High risk count",      int(df["high_risk_count"].sum()))
k3.metric("Open findings",        int(df["open_findings_count"].sum()))

# Trend chart
fig = px.bar(
    df.sort_values("risk_score_change_3m"),
    x="entity_name", y="risk_score_change_3m",
    color="risk_trend_direction",
    color_discrete_map={"Improving": "green", "Deteriorating": "red", "Stable": "gray"},
    title="Risk score change over last 3 months by entity",
    labels={"risk_score_change_3m": "Score change", "entity_name": "Entity"}
)
st.plotly_chart(fig, use_container_width=True)

st.dataframe(df, use_container_width=True)