import json
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
import plotly.express as px

st.set_page_config(layout="wide")
st.title("📊 Real-Time Sentiment Dashboard")

consumer = KafkaConsumer(
    "processed_reviews",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

data = []

placeholder = st.empty()

for msg in consumer:
    data.append(msg.value)

    if len(data) > 500:
        data = data[-500:]

    df = pd.DataFrame(data)

    with placeholder.container():
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Total messages", len(df))

        with col2:
            st.metric(
                "Positive %",
                round((df["sentiment"] == "positive").mean() * 100, 2)
            )

        fig = px.histogram(df, x="sentiment")
        st.plotly_chart(fig, width='stretch')  # Замена use_container_width=True

        col3, col4 = st.columns(2)

        with col3:
            # Гистограмма рейтингов
            if "rating" in df.columns:
                fig_rating = px.histogram(df, x="rating", title="Rating Distribution")
                st.plotly_chart(fig_rating, width='stretch')

        with col4:
            # Тренд настроений по времени
            if "timestamp" in df.columns:
                df["time_group"] = pd.to_datetime(df["timestamp"]).dt.floor("5min")
                sentiment_trend = df.groupby("time_group")["sentiment"].apply(
                    lambda x: (x == "positive").mean()
                ).reset_index()
                fig_trend = px.line(sentiment_trend, x="time_group", y="sentiment",
                                    title="Positive Sentiment Trend")
                st.plotly_chart(fig_trend, width='stretch')