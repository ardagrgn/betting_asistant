import http.client
import json
import pandas as pd
import requests
import os
import pickle
import time


class api_management(http.client.HTTPSConnection):
    
    def __init__(self):
        
        http.client.HTTPSConnection.__init__(self,"v3.football.api-sports.io")


        self.headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': "686a61aa0db8eda62ee605604fe52e77"
        }

    
    def quota_info(self):
        
        self.request("GET", "/status", headers=self.headers)
        res = self.getresponse()
        data = res.read()
        status=json.loads(data.decode("utf-8"))
        self.used=status["response"]["requests"]["current"]
        self.quota=status["response"]["requests"]["limit_day"]
        self.remains= self.quota-self.used
        print("used : {}, quota : {}, remains : {}".format(self.used,self.quota,self.remains))
        return self.remains

    
    def get_leagues(self):
    
        self.request("GET", "/leagues", headers=self.headers)
        res = self.getresponse()
        data = res.read()
        league_res=json.loads(data.decode("utf-8"))["response"]
        league_df=pd.json_normalize(league_res,record_path="seasons",meta=[["league","id"],["league","name"], ["country","name"],["league","type"] ])
        only_leagues= league_df[league_df["league.type"]=="League"][
        ["year","coverage.fixtures.events","coverage.fixtures.statistics_fixtures",
         "league.name","league.id","country.name"]]
        only_leagues["Coverage"]=  only_leagues[[
        "coverage.fixtures.events",
        "coverage.fixtures.statistics_fixtures"]].applymap(lambda x : 1 if x==True else 0).sum(axis=1)
        leagues_acc= only_leagues[only_leagues["Coverage"]==2]
        self.quota_info()
        return leagues_acc
    
    def get_fixtures(self,season,league):
   
        self.request("GET", "/fixtures?season={}&league={}".format(season,league), headers=self.headers)

        res = self.getresponse()
        data = res.read()
        fixture_res=json.loads(data.decode("utf-8"))["response"]
        self.quota_info()
        return fixture_res
    
    def  fix_stat(self,fixture):
        self.request("GET", "/fixtures/statistics?fixture={}".format(fixture), headers=self.headers)

        res = self.getresponse()
        data = res.read()
        stat_res=json.loads(data.decode("utf-8"))["response"]
        for k in range(len(stat_res)):
            stat_res[k]["statistics"]={i["type"]:i["value"] for i in stat_res[k]["statistics"]}
            stat_res[k]["fixture"]= {"id":fixture}
        
        return stat_res
    def fix_events(self,fixture):
        self.request("GET", "/fixtures/events?fixture={}".format(fixture), headers=self.headers)

        res = self.getresponse()
        data = res.read()
        event_res=json.loads(data.decode("utf-8"))["response"]
        for k in range(len(event_res)):
            event_res[k]["fixture"]= {"id":fixture}
        return event_res
    


selected_leagues=[61, 144,  71,  39,  78, 135,  88,  94, 140,  62, 203, 197,  79,
       188, 218, 119,  40,  41,  42,  98,  72, 141, 136, 103,  89, 113,
       169, 207, 210, 235, 292, 307, 106, 265, 323, 475]


data_dir="betting_asistant\\Documantation\\Data\\"

api= api_management()

leagues=api.get_leagues()
leagues.to_csv("{}leagues.csv".format(data_dir),index=False)



#We will use rate of change in time/calls. Becouse we will observe given time change according to one api call
call_rate= 60/300



try:
    
    saved_fixtures_df=pd.read_csv("{}saved_fixtures.csv".format(data_dir))
except:
    saved_fixtures_df=[]


saved_fixtures_df=saved_fixtures_df[saved_fixtures_df["league.id"].isin(selected_leagues)]


#MAX season for each leagues
max_fixtures=saved_fixtures_df.groupby("league.id",as_index=False
                                       )["league.season"
                                         ].max().rename({"league.season":"max_season"},axis=1)

last_seasons=leagues.merge(max_fixtures)
last_seasons_v1=last_seasons[last_seasons.max_season<=last_seasons["year"]
                             ].sort_values(["league.id","year"]).drop("max_season",axis=1)

prior_saved_fixtures=saved_fixtures_df.merge(max_fixtures)
prior_saved_fixtures=prior_saved_fixtures[prior_saved_fixtures["league.season"]<prior_saved_fixtures.max_season]


saved_fixtures_download=[]
quota=api.quota_info()-10

for i in range(last_seasons_v1.shape[0]):
    season=last_seasons_v1.iloc[i]["year"]
    league=last_seasons_v1.iloc[i]["league.id"]
    current=time.time()
    saved_fixtures_download=saved_fixtures_download+api.get_fixtures(season,league)
    diff=time.time()-current
    if diff < call_rate:
        time.sleep(0.2)
    print(quota,league,season,i)



post_saved_fixtures=pd.json_normalize(saved_fixtures_download
                                      ).drop(['teams.home.logo',
                                              'teams.away.logo','league.flag'],axis=1)

saved_fixtures_df= pd.concat([prior_saved_fixtures,post_saved_fixtures]).drop("max_season",axis=1)

saved_fixtures_df.to_csv("{}saved_fixtures.csv".format(data_dir),index=False)

finished_matches=['Match Finished', 'Technical loss' ]

relevant_saved_fixtures=saved_fixtures_df[(saved_fixtures_df["fixture.status.long"].isin(finished_matches))
                                         & (saved_fixtures_df["league.season"]>=2023)]

quota=api.quota_info()-10

try:
    
    fixture_stat=pd.read_csv("{}fixture_stat.csv".format(data_dir))
    
except:
    
    fixture_stat=[]

full_stats=fixture_stat

downloaded_stat_fixs=list(full_stats["fixture.id"].unique()) 

not_downloaded_stat_fixs=list(relevant_saved_fixtures["fixture.id"
                                                      ][relevant_saved_fixtures["fixture.id"
                                                                                ].isin(downloaded_stat_fixs)==False
                                                                                ].sort_values().unique())

quota=api.quota_info()-10

fixture_json=[]

iterasyon=0
for i in not_downloaded_stat_fixs:
    
    if iterasyon<quota:
        
        
        current=time.time()
        fixture_json=fixture_json+api.fix_stat(i)
        diff=time.time()-current
        
        if diff > call_rate:
            time.sleep(0.2)

        
    print(iterasyon)   
    iterasyon+=1
post_fixture_stat_df= pd.json_normalize(fixture_json)

fixture_stat_df=pd.concat([full_stats,post_fixture_stat_df])

fixture_stat_df.to_csv("{}fixture_stat.csv".format(data_dir),index=False)

downloaded=list(fixture_stat_df["fixture.id"].unique())

check=saved_fixtures_df[(saved_fixtures_df["fixture.id"].isin(downloaded)) & 
                       (saved_fixtures_df["fixture.status.long"]=='Match Finished')]

stat_groups=check.groupby(["league.season",
                           'league.id', 'league.name',
                           'league.country'],as_index=False)["fixture.id"].count()

all_groups=saved_fixtures_df[(saved_fixtures_df["fixture.status.long"
                                                ]=='Match Finished')].groupby(["league.season",
                                                                               'league.id', 'league.name',
                                                                               'league.country'
                                                                               ],as_index=False)["fixture.id"].count()

try:
    
    fixture_event=pd.read_csv("{}fixture_event_df.csv".format(data_dir))
    
except:
    
    fixture_event=[]

prior_fixture_event=fixture_event

downloaded_stat_events=list(prior_fixture_event["fixture.id"].unique()) 
not_downloaded_stat_events=list(relevant_saved_fixtures["fixture.id"
                                                        ][relevant_saved_fixtures["fixture.id"
                                                                                  ].isin(downloaded_stat_events)==False
                                                                                  ].sort_values().unique())


quota=api.quota_info()-10

iterasyon=0
post_event_json=[]

for i in not_downloaded_stat_events:
    
    if iterasyon<quota:
        
        
        current=time.time()
        post_event_json=post_event_json+api.fix_events(i)
        diff=time.time()-current
        
        if diff < call_rate:
            time.sleep(0.2)
        
        

    print(iterasyon,i)   
    iterasyon+=1
post_fixture_event_df= pd.json_normalize(post_event_json).drop("team.logo",axis=1)

fixture_event_df=pd.concat([prior_fixture_event,post_fixture_event_df])
fixture_event_df.to_csv("{}fixture_event_df.csv".format(data_dir),index=False)