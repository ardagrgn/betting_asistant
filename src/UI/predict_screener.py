import streamlit as st
import pandas as pd

# Load your datasets
predictions_df = pd.read_excel("predicts.xlsx")  # Your predictions data
fixtures_df = pd.read_csv("saved_fixtures.csv")        # Your fixture details

st.set_page_config(layout="wide")

if "selected_ids" not in st.session_state:
    st.session_state.selected_ids = set()


fixtures_df=fixtures_df[["fixture.id", 
         "fixture.date", "fixture.timezone",
         "fixture.referee",
         "fixture.venue.name",
           "fixture.venue.city",
           "league.logo",
           "league.season",
               ]]

fixtures_df["short_date"]=fixtures_df["fixture.date"].str[:10]
fixtures_df["fixture_time"]=fixtures_df["fixture.date"].str[11:16]




# Merge predictions with fixtures on fixture.id
df = pd.merge(predictions_df, fixtures_df, on="fixture.id", how="left")

# Sidebar filters
st.sidebar.header("Filter Fixtures")
selected_date = st.sidebar.date_input("Select date",max_value=df.short_date.max(), 
                                      min_value=df.short_date.min(), value=None)

selected_league = st.sidebar.selectbox("Select league", df["league.name"].unique())
selected_team = st.sidebar.text_input("Search by team")

selected_order = st.sidebar.selectbox("Order By", ["Fixture Date","Over 2.5 Odds","Bellow 2.5 Odds"] )

col_df=df[["short_date","fixture.venue.city","fixture.venue.name",
                            "teams.home.name","teams.away.name","preds",
                            "Lower_q","Upper_q","odd_up","odd_down"
                            ]].rename({
    "short_date":"Fixture Date", "teams.home.name":"Home Team", 
    "teams.away.name":"Away Team",
    "fixture.venue.name":"Venue", "fixture.venue.city":"City", 
    "preds":"Prediction", "Lower_q":"Lower Quantile", 
    "Upper_q":"Upper Quantile", "odd_up":"Over 2.5 Odds", "odd_down":"Bellow 2.5 Odds"
},axis=1)



# Apply filters

if selected_date:
    filtered_df = col_df[
        (df["short_date"] == selected_date.strftime("%Y-%m-%d")) &
        (df["league.name"] == selected_league) &
        (df["teams.home.name"].str.contains(selected_team, case=False) | 
        df["teams.away.name"].str.contains(selected_team, case=False))
    ]

else:
    filtered_df = col_df[
        (df["league.name"] == selected_league) &
        (df["teams.home.name"].str.contains(selected_team, case=False) | 
        df["teams.away.name"].str.contains(selected_team, case=False))
    ]

# Main view
st.title("Football Prediction Dashboard")

filtered_df["Select"] = False

filtered_df["id"] = filtered_df.index


filtered_df_v1=filtered_df[["Select","id"]+ list(filtered_df.columns[:-2]) ].copy()

filtered_df_v1.sort_values(selected_order,inplace=True,ascending=False)

edited_df=st.data_editor(filtered_df_v1, use_container_width=True,
    num_rows="dynamic",
    disabled=filtered_df_v1.columns[2:] )

new_selected_ids = set(edited_df.loc[edited_df["Select"], "id"])
st.session_state.selected_ids.update(new_selected_ids)

selected_rows_df = col_df.loc[list(st.session_state.selected_ids)]

if st.button("Reset Selection"):
    st.session_state.selected_ids.clear()

if selected_rows_df.shape[0]>0:
    st.markdown("### Selected Matches")
    st.dataframe(selected_rows_df, use_container_width=True)