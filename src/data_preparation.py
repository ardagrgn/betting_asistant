import numpy as np
import pandas as pd
import pickle
from scipy.stats import poisson as po
pd.set_option("display.max_column",None)

data_dir="Documantation\\Datas\\"

fixtures= pd.read_csv("{}saved_fixtures.csv".format(data_dir))


#FIXTURES TO PLAY

fixs_to_play=fixtures[fixtures["fixture.status.long"]=="Not Started"]
fixs_to_play["fixture_date"]=fixs_to_play["fixture.date"].str[:10]

fixs_to_play_v1=fixs_to_play[["fixture.id","fixture_date",
                              "league.season","league.id",
                              "teams.home.id","teams.away.id"]]


# In order to gather average performance information for given team 
# We will duplicate each fixture for both away and home team

home_teams=fixs_to_play_v1.copy()
home_teams["team_id"]=home_teams["teams.home.id"]
home_teams["Home"]=1

away_teams=fixs_to_play_v1.copy()
away_teams["team_id"]=away_teams["teams.away.id"]
away_teams["Home"]=0

fixs_to_play_v2=pd.concat([away_teams,home_teams])


#Bellow we find closset in coming fixture for given team
fixs_to_play_v3=fixs_to_play_v2.sort_values(["fixture_date",
                                             "fixture.id","team_id"]).drop_duplicates("team_id",
                                             keep="first")


#We expect thoose selected fixtures be dublicated.
#Otherwise means, there is problem becouse on team played its fixture and oppose team didn't
fixs_dub=fixs_to_play_v3.groupby("fixture.id").team_id.count()
dub=list(fixs_dub[fixs_dub>1].index)

#We get final dataframe object for using at feature eng.
fixs_to_play_v4=fixs_to_play_v3[fixs_to_play_v3["fixture.id"].isin(dub)]


#FT= Full Time

# We select finished matches

fixtures_ft=fixtures[fixtures["fixture.status.short"]=="FT"].drop(['score.extratime.home',
                                                                   'score.extratime.away',
                                                                   'score.penalty.home',
                                                                   'score.penalty.away',
                                                                   'fixture.venue.id',
                                                                   'fixture.venue.name',
                                                                   'fixture.timestamp', 
                                                                   'fixture.periods.first', 
                                                                   'fixture.periods.second',
                                                                   'league.logo',"fixture.referee"
                                                                  ],axis=1)


#We assing 1 for True and 0 for False
fixtures_ft[["teams.home.winner",
             "teams.away.winner"]]= fixtures_ft[["teams.home.winner",
                                                 "teams.away.winner"]].applymap(lambda x : 1 if x  else 0)

#USA wont be included becouse their fixture system very differenrt than other nationals
#Czech- Republic won't be included becouse some of features missing in their data

fixtures_ft_v1=fixtures_ft[fixtures_ft["league.country"].isin(["World","Europe",'USA',"Czech-Republic"])==False]

#In some leagues there is promotion and demotion games. 
#We only focus on regular season games
fixtures_ft_v2= fixtures_ft_v1[(fixtures_ft_v1["league.round"].str[:14]== 'Regular Season') ]

#Fixture.date has format "YYYY-MM-DD HH:MM:SS"
#We extract date and hour from the column
fixtures_ft_v2["fixture_date"]=fixtures_ft_v2["fixture.date"].str[:10]
fixtures_ft_v2["fixture_start_time"]=fixtures_ft_v2["fixture.date"].str[11:19]
fixtures_ft_v2.drop("fixture.date",axis=1,inplace=True)

fixtures_ft_v2["home_point"]=fixtures_ft_v2["teams.home.winner"]*3
fixtures_ft_v2["away_point"]=fixtures_ft_v2["teams.away.winner"]*3

#In draw games, both team recorded as winner.
# We detec record where both team got 3 points and correct them
fixtures_ft_v2["home_point"][(fixtures_ft_v2.home_point==fixtures_ft_v2.away_point)]=1
fixtures_ft_v2["away_point"][fixtures_ft_v2.home_point==1]=1
fixtures_ft_v2["teams.away.winner"][fixtures_ft_v2.home_point==1]=0
fixtures_ft_v2["teams.home.winner"][fixtures_ft_v2.home_point==1]=0

#We import fixtures with most recent statistics
fixture_stat_df= pd.read_csv("{}fixture_stat.csv".format(data_dir)).drop(["team.logo",'statistics.Passes %',
                                                       'statistics.expected_goals',
       'statistics.Assists', 'statistics.Counter Attacks',
       'statistics.Cross Attacks', 'statistics.Free Kicks', 'statistics.Goals',
       'statistics.Goal Attempts', 'statistics.Substitutions',
       'statistics.Throwins', 'statistics.Medical Treatment'],axis=1)

# In data missing of a feature value means, there was no value to observe
#But we will not apply this loggic on total shots and total passes
fixture_stat_df.fillna(0,inplace=True)

#We get fixture date and team infos for give fixture
relevant_fixtures=fixtures_ft_v2[["league.season","league.name",
                                  "league.id","fixture.id","fixture_date",
                                  "teams.away.id","teams.home.id",
                                  "goals.home","goals.away"]]

fixture_stat_v1= fixture_stat_df.merge(relevant_fixtures)

fixture_stat_v1["Goal"]= fixture_stat_v1.apply(lambda x: x["goals.away"] if x["teams.away.id"]==x["team.id"] else x["goals.home"],
                                               axis=1)
#SELF STATISTICS

#At Ball Possesion we change format from xx% to xx
self_stat= fixture_stat_v1.copy().drop(["goals.home","goals.away"],axis=1)
self_stat["statistics.Ball Possession"]=self_stat["statistics.Ball Possession"].str[:-1].astype("float")/100

# We will use accurate pass rate instead of accurate passes.
self_stat["Accurate_Pass_Rate"]= round(self_stat["statistics.Passes accurate"]/self_stat["statistics.Total passes"],2)


#Shot Metrics

# In order to find shot on goal rate, first we find unblocked shots
self_stat["Shot_On_Goal_Rate"]=round(self_stat["statistics.Shots on Goal"]/(self_stat["statistics.Total Shots"]-self_stat["statistics.Blocked Shots"]),2)

self_stat["Inside_Box_Shot_Rate"]=round(self_stat["statistics.Shots insidebox"]/self_stat["statistics.Total Shots"],2)


self_stat_v1=self_stat.drop(["statistics.Total Shots","statistics.Blocked Shots"],axis=1)

#Other Metrics
 
self_stat_v1["team_id"]=self_stat_v1["team.id"]


self_stat_v1.rename({'statistics.Fouls':"Foul", 'statistics.Corner Kicks':"Corner_Kicks",
       'statistics.Offsides':"Offsides", 'statistics.Ball Possession':"Ball_Possession_Rate",
       'statistics.Yellow Cards':"Yellow_Cards", 'statistics.Red Cards':"Red_Cards",
        'statistics.Total passes':"Total_Passes","statistics.Shots on Goal": "Shots_on_Goal",
                     "statistics.Shots off Goal":"Shots_off_Goal"
       },axis=1,inplace=True)
self_stat_v1.drop(['statistics.Goalkeeper Saves','statistics.Passes accurate',
                   "statistics.Shots insidebox","statistics.Shots outsidebox","team.id"],axis=1,inplace=True)

self_stat_v1["Home"]=0
self_stat_v1["Home"][self_stat_v1["teams.home.id"]==self_stat_v1["team_id"]]=1

#OPPOSE STATISTICS

# In this section we will create features which shows 
#How given team played againts opposed team

oppose_stat= fixture_stat_v1[["statistics.Shots on Goal",
                              "statistics.Total Shots","statistics.Blocked Shots",
                             "statistics.Offsides",
                             "statistics.Total passes","statistics.Passes accurate",
                            "fixture.id"]]

def oppose_team(x):
    
    if x["team.id"]==x["teams.away.id"]:
        return x["teams.home.id"]
    else:
        return x["teams.away.id"]
    

#First we detect opposed team 
oppose_stat["team_id"]=fixture_stat_v1[["team.id",
                                        "teams.home.id","teams.away.id"]].apply(lambda x : oppose_team(x),axis=1)

oppose_stat_v1=oppose_stat.copy()

#In this operation, we can see what kind of operation we will do
#Normaly a feature (statistics.Offsides) show opposed team's how many time fallen into offside
#But in sake of oppose stat we  call this feature "trapped_offside"
#Now we can think this feature as  how many time given teams trapped opposed team into offside
oppose_stat_v1["trapped_offside"]=oppose_stat_v1["statistics.Offsides"]
oppose_stat_v1.drop("statistics.Offsides",axis=1,inplace=True)

oppose_stat_v1["Pass_Forced_to_Fail"]=round(
    (oppose_stat_v1["statistics.Total passes"]-oppose_stat_v1["statistics.Passes accurate"]
     )/oppose_stat_v1["statistics.Total passes"],2)
oppose_stat_v1.drop(["statistics.Total passes","statistics.Passes accurate"],axis=1,inplace=True)

oppose_stat_v1["Blocked_Shot_Rate"]=round(
    oppose_stat_v1["statistics.Blocked Shots"]/oppose_stat_v1["statistics.Total Shots"],2)


oppose_stat_v1["Total_Shot_Taken"]=oppose_stat_v1["statistics.Shots on Goal"]
oppose_stat_v1.drop(["statistics.Total Shots","statistics.Blocked Shots"],axis=1,inplace=True)

#In order to find goal saved percentage, we need to get goalkeeper saves for given team
fix=fixture_stat_v1[["team.id","fixture.id","statistics.Goalkeeper Saves"]].rename({"team.id":"team_id"},axis=1)

oppose_stat_v2=oppose_stat_v1.merge(fix)

oppose_stat_v2["Goal_Saved_Percentage"]=round(
    oppose_stat_v2["statistics.Goalkeeper Saves"]/oppose_stat_v2["statistics.Shots on Goal"],2)
oppose_stat_v2.drop(["statistics.Goalkeeper Saves","statistics.Shots on Goal"],axis=1,inplace=True)

final_stat_data=self_stat_v1.merge(oppose_stat_v2)

#EVENTS DATA

#At this section we define events of given team in given fixture

events_df= pd.read_csv("{}fixture_event_df.csv".format(data_dir))

#Event details below will be focused
events_df_v1=events_df[events_df.detail.isin(['Yellow Card', 'Red Card','Normal Goal', 'Own Goal', 'Penalty'])]

#Own goal and Penalty are rare events. We treat them as normal goal
events_df_v1["detail"][events_df_v1["detail"].isin(['Own Goal', 'Penalty'])]="Normal Goal"

#In temporal sense, we will dive our events in 6 groups.
#Which are 15 minutes intervals

def interval(x):
    
    if 0<=x<=15:
        return "1st_Quarter"
    elif 16<=x<=30:
        return "2nd_Quarter"
    elif 31<=x<=45:
        return "3rd_Quarter"
    
    elif 46<=x<=60:
        return "4th_Quarter"
    
    elif 61<=x<75:
        return "5th_Quarter"
    
    elif 76<=x<=90:
        return "6th_Quarter"
    else:
        pass


events_df_v1["time_intervals"]= events_df_v1["time.elapsed"].apply(lambda x: interval(x))

events_df_v1["Occurance"]=1

events_count= events_df_v1.groupby(["fixture.id","time_intervals",
                                    "detail","team.id","team.name"],as_index=False).Occurance.sum()

def t(x):
    col=x["time_intervals"]+"_"+x["detail"]
    trans=pd.DataFrame(index=col,data=x["Occurance"].values).transpose()
    return trans

events_trans=events_count.groupby(["fixture.id","team.id"]).apply(lambda x : t(x))

events_trans_v1=events_trans.fillna(0).reset_index().drop("level_2",axis=1)

çoklama=events_trans_v1.groupby("fixture.id")["team.id"].count()


concieved=events_trans_v1[["fixture.id","team.id",'1st_Quarter_Normal Goal',
                '2nd_Quarter_Normal Goal',
                '3rd_Quarter_Normal Goal',
                '4th_Quarter_Normal Goal',
                '5th_Quarter_Normal Goal',
                '6th_Quarter_Normal Goal']]

liste=['1st_Quarter_Normal Goal',
                '2nd_Quarter_Normal Goal',
                '3rd_Quarter_Normal Goal',
                '4th_Quarter_Normal Goal',
                '5th_Quarter_Normal Goal',
                '6th_Quarter_Normal Goal']

söz={}       

#söz[liste[0]]
for i in liste:
    söz[i]= i[:12]+'Conceived'+ i[-5:]


concieved.rename(söz,inplace=True,axis=1)

#In occurence data, its posible that only one team has event record
#That is why we gather team id and fixture id info from  statistics data
teams=final_stat_data[["fixture.id","team_id"]].rename({"team_id":"team.id"},axis=1)

concieved_v1= teams.merge(concieved,how="left")

#First we get our events, after we get our concived goal events
events_trans_v2=teams.merge(events_trans_v1,how="left").merge(concieved_v1,on="fixture.id",how="left")

#But in concieved event, fixture id must match but team id's sholdnt  match.
#Becouse concieved event is orginally opposed teams normal goals
events_trans_v3=events_trans_v2[events_trans_v2["team.id_x"]!=events_trans_v2["team.id_y"]].drop(
    "team.id_y",axis=1).rename({"team.id_x":"team_id"},axis=1)

#EVENT AND STATS DATA

event_stat=final_stat_data.merge(events_trans_v3).fillna(0)

#We get fixture infos to our fixture/team features
event_fix=event_stat.merge(fixtures_ft_v2[["fixture.id",
                                           "league.id","league.season"]]).sort_values(["league.id",
                                                                                       "league.season","fixture_date"])

event_fix_v1=pd.concat([event_fix,
                        fixs_to_play_v4]).sort_values(["league.id","league.season",
                                                       "team_id","fixture_date"])

mean_cols= ['Shots_on_Goal', 'Shots_off_Goal', 
       'Foul', 'Corner_Kicks', 'Offsides',
       'Ball_Possession_Rate', 'Yellow_Cards', 'Red_Cards', 'Total_Passes',
          'Accurate_Pass_Rate', 'Shot_On_Goal_Rate', 'Inside_Box_Shot_Rate',
           'trapped_offside',
       'Pass_Forced_to_Fail', 'Blocked_Shot_Rate', 'Total_Shot_Taken',
       'Goal_Saved_Percentage' ]
possion_cols= ['1st_Quarter_Normal Goal', '4th_Quarter_Yellow Card',
       '5th_Quarter_Yellow Card', '6th_Quarter_Normal Goal',
       '6th_Quarter_Yellow Card', '2nd_Quarter_Normal Goal',
       '3rd_Quarter_Yellow Card', '1st_Quarter_Yellow Card',
       '3rd_Quarter_Normal Goal', '4th_Quarter_Normal Goal',
       '2nd_Quarter_Yellow Card', '3rd_Quarter_Red Card',
       '5th_Quarter_Normal Goal', '5th_Quarter_Red Card',
       '6th_Quarter_Red Card', '4th_Quarter_Red Card', '2nd_Quarter_Red Card',
       '1st_Quarter_Red Card', '1st_Quarter_Conceived Goal',
       '2nd_Quarter_Conceived Goal', '3rd_Quarter_Conceived Goal',
       '4th_Quarter_Conceived Goal', '5th_Quarter_Conceived Goal',
       '6th_Quarter_Conceived Goal']

means=event_fix_v1.groupby(["league.id",
                            "league.season","team_id"],as_index=False)[
                                mean_cols].rolling(4,min_periods=1,closed="left").median()

def poi(x):
    
    mu=x.mean()
    
    return 1-po.pmf(0,mu=mu)

poisson_df= event_fix_v1.groupby(["league.id","league.season","team_id",
                              ],as_index=False)[possion_cols].rolling(4,min_periods=1,closed="left").agg(poi)

#Lagged Data

team_fix=event_fix_v1[['fixture.id',
       'fixture_date', 'teams.away.id', 'teams.home.id','team_id', 'Home','team.name']]
lagg_data= pd.concat([team_fix,means.iloc[:,3:],poisson_df.iloc[:,3:]],axis=1)

#With 0 mean of Total Shot means there is problem becouse of the data provider.
#But this problem doesn't all records for given teams. We will impute only records with 0 mean in Total Shot
lagg_data=lagg_data[(lagg_data.Total_Passes!=0) & (lagg_data.Total_Passes.isna()==False)]

#Score And Rank Table 

away_fix= fixtures_ft_v2[['fixture_date','league.country','league.id','league.name','league.season',
                      'teams.away.id','teams.away.name',
                      'goals.away','goals.home','away_point']].rename({"teams.away.id":"team_id",
                                                                     "teams.away.name":"team_name",
                                                                    "away_point":"point"}
                                                                    ,axis=1)

away_fix["Average"]= away_fix["goals.away"]-  away_fix["goals.home"] 
away_fix_v1= away_fix.drop(["goals.away","goals.home"],axis=1)

home_fix= fixtures_ft_v2[['fixture_date','league.country','league.id','league.name','league.season',
                      'teams.home.id','teams.home.name',
                      'goals.away','goals.home','home_point']].rename({"teams.home.id":"team_id",
                                                                    "teams.home.name":"team_name",
                                                                    "home_point":"point"}
                                                                    ,axis=1)

home_fix["Average"]=   home_fix["goals.home"] - home_fix["goals.away"] 
home_fix_v1= home_fix.drop(["goals.away","goals.home"],axis=1)

final_fix= pd.concat([away_fix_v1,home_fix_v1])
final_fix["Match_Played"]=1

#For clubs with plural name, we will use the one with most frequent
names=final_fix.groupby(["team_id","team_name"],as_index=False)["point"].count().sort_values("point",ascending=False)
names_unique= names[["team_id","team_name"]].drop_duplicates("team_id",keep="first")

final_fix_v1= final_fix.drop(["team_name"],axis=1).merge(names_unique)
season_leagues= final_fix_v1[["league.season","league.id"]].drop_duplicates().reset_index(drop=True)


tables=[]
for i in range(season_leagues.shape[0]):
    season=season_leagues.iloc[i,0]
    league= season_leagues.iloc[i,1]
    
    given_ls= final_fix_v1[(final_fix_v1["league.season"]==season) & 
                         (final_fix_v1["league.id"]==league)]

    teams_of_season= given_ls[["league.id","league.season","team_id","team_name"]].drop_duplicates()

    dates_of_season=  given_ls[["league.id","league.season","fixture_date","league.country","league.name"]].drop_duplicates()

    cross_date_team= teams_of_season.merge(dates_of_season)

    tables.append(cross_date_team.merge(given_ls,how="left").fillna(0))


pre_table= pd.concat(tables).sort_values(["league.id","team_id","fixture_date"])

cumsums= pre_table.groupby(["league.country","league.name","league.id",
                                 "league.season","team_id","team_name"])[["point",
                                                                          "Average","Match_Played"]].cumsum()

# After we get points and averages, we will cumsum for given at for all tables in the season
pre_table_v1= pd.concat([pre_table.iloc[:,:7],cumsums],axis=1).sort_values(["league.id","fixture_date",
                                                                            "point","Average","team_name"],
                                                                          ascending=[True,True,False,False,True])

pre_table_v1["Rank"]=1

ranks=pre_table_v1.groupby(["league.id","league.season","fixture_date"],as_index=False)[["Rank"]].cumsum()
pre_table_v1["Rank"]=ranks

up_point=pre_table_v1.groupby(["league.id",
                               "league.season","fixture_date"],as_index=False)[["point",
                                                                                "Match_Played"]
                                                    ].rolling(1,closed="left").agg("min").rename({"point":"up_point",
"Match_Played":"Up_MP"},axis=1)
up_point["team_id"]= pre_table_v1["team_id"]

samp_sorted= pre_table_v1.sort_values(["league.id",
                        "league.season","fixture_date","Rank"],ascending=False).reset_index(drop=True)
down_point=samp_sorted.groupby(["league.id",
                    "league.season","fixture_date"],as_index=False)[["point",
                            "Match_Played"]].rolling(1,closed="left").agg("min").rename({"point":"down_point",
                                                                                "Match_Played":"Down_MP"},axis=1)
down_point["team_id"]= samp_sorted["team_id"]

pre_table_v2 =pre_table_v1.merge(up_point).merge(down_point)

#In order to express point need for given match we get point difference from above team and below team.
#We also consider played match diference too

pre_table_v2["up_point_diff"]= (pre_table_v2.up_point-pre_table_v2.point) + (pre_table_v2.Match_Played-pre_table_v2.Up_MP)*3
pre_table_v2["down_point_diff"]=  pre_table_v2.down_point-pre_table_v2.point  +(pre_table_v2.Match_Played-pre_table_v2.Down_MP)*3
pre_table_v3=pre_table_v2.drop(["up_point","down_point","Up_MP","Down_MP"],axis=1)

pre_table_v3["min_rank"]=pre_table_v3.groupby(["league.id","league.season"],as_index=False).Rank.transform("max")

def rank_kat(x):
    
    if x.Rank==1:
        return "title"
    
    elif 1<x.Rank<=3:
        
        return "first_2_3"
    
    elif 4<= x.Rank <=6:
        return "first_4_6"
    
    elif 7<=x.Rank<=x.min_rank-3:
        return "Middle"
    
    elif x.min_rank-2<=x.Rank<= x.min_rank-1:
        return "last_2_3"
    else:
        return "last"
    

pre_table_v3["Rank_Name"]=pre_table_v3[["Rank","min_rank"]].apply(lambda x: rank_kat(x),axis=1)

def up_names(x):
    
    if str(x)=="nan":
        return "top"
    elif x<=0:
        return "1_point_needed"
    
    elif 0<x<=3:
        return "3_point_for_catch"
    else:
        return "3_point_for_chase"
def down_names(x):
    
    if str(x)=="nan":
        return "bottom"
    elif x>-3:
        return "3_point_for_run"
    elif x==-3:
        return "1_point_for_run"
    else:
        return "comfort"
    

pre_table_v3["Rise_Need"]=pre_table_v3.up_point_diff.apply(lambda x: up_names(x))
pre_table_v3["Fall_Prevent"]= pre_table_v3.down_point_diff.apply(lambda x: down_names(x))

pre_table_v4=pre_table_v3[['league.id', 'league.season', 'team_id', 'team_name', 'fixture_date',
       'league.country', 'league.name','Rank',"point","Rank_Name","Rise_Need","Fall_Prevent","Match_Played"]]

#MAIN DATA

#Lagged Data

# At given fixture there shouldnt be any record with null or zero ball posession rate or total shot
no_null_lagg=lagg_data[(lagg_data.Total_Passes.notna()) ]

# In order to create final data we will select home team and away team.
#And merge them into one record

home_lagged=no_null_lagg[no_null_lagg.Home==1].drop(["team_id","team.name","Home","teams.away.id"],axis=1)
home_columns=[]

for i in range(len(home_lagged.columns)):
    
               
    if i>2:
               
               
        home_columns.append("{}_Oppose".format(home_lagged.columns[i]))
               
    else:
        home_columns.append(home_lagged.columns[i])

               
home_lagged.columns= home_columns 

away_lagged=no_null_lagg[no_null_lagg.Home==0].drop(["team_id","team.name","Home","teams.home.id"],axis=1)
away_columns=[]

for i in range(len(away_lagged.columns)):
    
               
    if i>2:
               
               
        away_columns.append("{}_Oppose".format(away_lagged.columns[i]))
               
    else:
        away_columns.append(away_lagged.columns[i])
                 
away_lagged.columns= away_columns

not_played_fixs=fixs_to_play[fixs_to_play["fixture.id"].isin(dub)][fixtures_ft_v2.columns[:-3]]

#We gather played fixtures and incoming fixtures and merge with stats/events

home=no_null_lagg[no_null_lagg.Home==1].drop(["teams.away.id","teams.home.id","team.name"],axis=1)
away=no_null_lagg[no_null_lagg.Home==0].drop(["teams.away.id","teams.home.id","team.name"],axis=1)

fixtures_w_lagged_home= pd.concat([fixtures_ft_v2,not_played_fixs]).merge(home).merge(away_lagged)
fixtures_w_lagged_away= pd.concat([fixtures_ft_v2,not_played_fixs]).merge(away).merge(home_lagged)

#Rank Data

#Now will gather most recent rank status before given match date
rank_dates=pre_table_v4[["league.id",
                         "league.season","team_id","fixture_date"]].rename({"fixture_date":"rank_date"},axis=1)

dates=pd.concat([fixtures_ft_v2,not_played_fixs])[["league.id",
                                                   "league.season","teams.away.id","teams.home.id","fixture_date"]]

home_dates=dates.drop("teams.away.id",axis=1).merge(rank_dates,left_on=["league.id",
                                                                        "league.season",
                                                                        "teams.home.id"],right_on=["league.id",
                                                                                                   "league.season",
                                                                                                   "team_id"])
home_dates_v1=home_dates[home_dates.fixture_date>home_dates.rank_date].groupby(["league.id",
                                                                                "league.season",
                                                                                "teams.home.id",
                                                                                "fixture_date",
                                                                                "team_id"],as_index=False).rank_date.max()

away_dates=dates.drop("teams.home.id",axis=1).merge(rank_dates,left_on=["league.id",
                                                                        "league.season",
                                                                        "teams.away.id"],right_on=["league.id",
                                                                                                   "league.season",
                                                                                                   "team_id"])
away_dates_v1=away_dates[away_dates.fixture_date>away_dates.rank_date].groupby(["league.id",
                                                                                "league.season",
                                                                                "teams.away.id",
                                                                                "fixture_date",
                                                                                "team_id"],as_index=False).rank_date.max()


rank_features=pre_table_v4.drop(["team_name",
                                 "league.country",
                                 "league.name","Rank","point"],axis=1).rename({"fixture_date":"rank_date"},axis=1)
dumms=pd.get_dummies(rank_features[["Rank_Name","Rise_Need","Fall_Prevent"]]).applymap(lambda x: 1 if x else 0)
rank_features_v1=pd.concat([rank_features.drop(["Rank_Name","Rise_Need","Fall_Prevent"],axis=1),dumms],axis=1)

home_rank=rank_features_v1.merge(home_dates_v1)
home_rank_v1=home_rank.drop(["teams.home.id","rank_date"],axis=1)
home_rank_oppose=home_rank.drop(["rank_date","team_id"],axis=1)
h_rank_columns=["{}_Oppose".format(i) for i in home_rank_oppose.columns[2:-2]]
home_rank_oppose.columns=list(home_rank_oppose.columns[:2])+h_rank_columns+["teams.home.id","fixture_date"]

away_rank=rank_features_v1.merge(away_dates_v1)
away_rank_v1=away_rank.drop(["teams.away.id","rank_date"],axis=1)
away_rank_oppose=away_rank.drop(["rank_date","team_id"],axis=1)
a_rank_columns=["{}_Oppose".format(i) for i in away_rank_oppose.columns[2:-2]]
away_rank_oppose.columns=list(away_rank_oppose.columns[:2])+a_rank_columns+["teams.away.id","fixture_date"]

away_data_final=fixtures_w_lagged_away.merge(away_rank_v1).merge(home_rank_oppose)
home_data_final=fixtures_w_lagged_home.merge(home_rank_v1).merge(away_rank_oppose)

datamart=pd.concat([home_data_final,away_data_final]).sort_values("fixture.id")

datamart_v1=datamart[datamart.Match_Played>4]

group=datamart_v1.groupby(["league.country",
                           "league.id","league.name","league.season"],as_index=False)["fixture.id"].count()

full_fixs=fixtures_ft_v2.groupby(["league.country",
                                  "league.id",""
                                  "league.name","league.season"],as_index=False)["fixture.id"].count()

datamart_v1["Goal"]=datamart_v1.apply(lambda x : x["goals.home"] if x["Home"]==1 else x["goals.away"],axis=1)

datamart_final=datamart_v1[(datamart_v1["Total_Passes"]>0) & (datamart_v1["Total_Passes_Oppose"]>0)].drop(
    ["goals.home","goals.away"],axis=1)

datamart_final=datamart_final[datamart_final["league.season"]>2015]

datamart_final.to_csv("{}datamart.csv".format(data_dir),index=False)