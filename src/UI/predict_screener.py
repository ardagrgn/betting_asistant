import streamlit as st
import pandas as pd

# Load your datasets
predictions_df = pd.read_excel("data/predicts.xlsx")  # Your predictions data
fixtures_df = pd.read_csv("data/saved_fixtures.csv")        # Your fixture details



fixtures_df=fixtures_df[["fixture.id", 
         "fixture.date", "fixture.timezone",
         "fixture.referee",
         "fixture.venue.name",
           "fixture.venue.city",
           "league.country",
           "league.logo",
           "league.season",
           "league.round"
               ]]

fixtures_df["short_date"]=fixtures_df["fixture.date"].str[:10]
fixtures_df["fixture_time"]=fixtures_df["fixture.date"].str[11:16]


# Merge predictions with fixtures on fixture.id
df = pd.merge(predictions_df, fixtures_df, on="fixture.id", how="left")

# Sidebar filters
st.sidebar.header("Filter Fixtures")
selected_date = st.sidebar.date_input("Select date",max_value=df.short_date.max(),min_value=df.short_date.min())
selected_league = st.sidebar.selectbox("Select league", df["league.name"].unique())
selected_team = st.sidebar.text_input("Search by team")


# Apply filters
filtered_df = df[
    (df["short_date"] == selected_date.strftime("%Y-%m-%d")) &
    (df["league.name"] == selected_league) &
    (df["teams.home.name"].str.contains(selected_team, case=False) | 
     df["teams.away.name"].str.contains(selected_team, case=False))
]

# Main view
st.title("Football Prediction Dashboard")

st.dataframe(filtered_df[[
    "fixture.date", "teams.home.name", "teams.away.name", 
    "preds", "Lower_q", "Upper_q", "odd_up", "odd_down",
    "fixture.venue.name", "fixture.venue.city"
]])