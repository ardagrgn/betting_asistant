from sklearn.preprocessing import Normalizer,StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, ElasticNet
import lightgbm as lg
from sklearn.svm import LinearSVC
import pandas as pd 
import numpy as np
import pickle
import os
from sklearn.model_selection import GridSearchCV as  gv
import optuna


def try_model(trymod):
            
    trymod.fit(y=otrain[target], X=otrain[list(input_cols)])
    pred=trymod.predict(otest[list(input_cols)])
    pred[pred<0]=0
   
    return np.mean(abs(pred-otest[target]).sum())


def model_obj(name):
    def objective(trial):

        if name=="lgbm":
            result=try_model(models["lgbm"]( 
                boosting_type=trial.suggest_categorical("boosting_type",
                                                        ["gbdt","dart"]),

                num_leaves= trial.suggest_int("num_leaves",
                                            3,50),
                max_depth=trial.suggest_int("max_depth",
                                            -1,20),
                learning_rate= trial.suggest_float("learning_rate",
                                                0.001,0.1),
                reg_alpha= trial.suggest_float("reg_alpha",
                                            0.0,0.1),

                reg_lambda= trial.suggest_float("reg_lambda",
                                                0.0,0.1),
                verbosity=-1
            ))

            return result      
        
        elif name=="elastic":
            result=try_model(models["elastic"]( 
                            alpha= trial.suggest_float("alpha",
                                                       0.5,5),
                            l1_ratio=trial.suggest_float("l1_ratio",
                                                         0.1,1),
                            fit_intercept=False

            
                            ) )
        
            return result
        
        
        
    return objective
        

models= {"lgbm":lg.LGBMRegressor,
         "elastic":ElasticNet,
         "lr":LinearRegression
         }

target= "Goal"

def fit_model(model,mod_dict,train,test):

    
    model_in=model(**mod_dict)
    model_in.fit(y=train[target], X=train[list(input_cols)])
    pred=model_in.predict(test[list(input_cols)])
    pred[pred<0]=0
    pred[pred>6]=6
   
    return np.mean(abs(pred-test[target])),model_in

def pred_model(model,main_data):
    pred=model.predict(main_data[list(input_cols)])
    pred[pred<0]=0
   
    return pred

def combinations(iterable, r):

    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)



def pred_ensembler(models,pre_dict):
    final_pred=0
    for m in  models:
        final_pred+= pre_dict[k]
    
    return final_pred/len(models)



def up_max(x):
    d=x[x>=0]
    if len(d)>0:
        return d.max()
    else:
        return 0

def down_min(x):
    d=x[x<0]
    if len(d)>0:
        return d.min()
    else:
        return 0

#Inserting Data 

data_dir="C:/Users/Arda/Downloads/Upwork/Portfolio/Betting_Asistant/data/"
data=pd.read_csv("{}datamart.csv".format(data_dir))

print("data loaded")

#Getting  relevant input feature data for models
feature_data=data.iloc[:,28:]

feature_data_v1=pd.concat([data[["fixture.id","league.id","league.season","team_id"]],
                            feature_data],axis=1)

#Applying train and test

fixtures=feature_data_v1["fixture.id"].unique()
fix_size= len(fixtures)


#Most recent finished matches will be used as test
train= feature_data_v1[data["fixture_date"]<="2025-01-31"]

test=feature_data_v1[data["fixture_date"]>"2025-01-31"]

input_cols=train.columns[4:-1]

#For parameter optimization purposes, we split our train data.
otrain,otest=train_test_split(train,test_size=0.3,random_state=42)


print("Hyperparameter optimization started for " , list(models.keys()), " models")

saves={}
for name in list(models.keys())[:-1]:
    
    study = optuna.create_study(direction="minimize")
    
    study.optimize(model_obj(name)
               , n_trials=30)
    
    saves[name]= study.best_params


print("Hyperparameter optimization finished")


lg_score,lg_model=fit_model(models["lgbm"],saves["lgbm"],train,test)
lg_preds=pred_model(lg_model,test)

ela_score,ela_model=fit_model(models["elastic"],saves["elastic"],train,test)
ela_preds=pred_model(ela_model,test)

lr_score,lr_model= fit_model(models["lr"],{},train,test)
lr_preds= pred_model(lr_model,test)

print("Models are fitted to full train data")

pred_dict= {"lr":lr_preds, "ela":ela_preds, "lg":lg_preds}

pkeys=list(pred_dict.keys())

searched=list(iter(combinations(pkeys,2)))+[[i] for i in pkeys]+[pkeys]

wining=[]
best_score=10

for i in  searched:
    final_pred=0
    print(i)
    
    for k in i:
        final_pred+= pred_dict[k]
    
    final_pred=final_pred/len(i)

    score=abs(final_pred-test["Goal"]).sum()/final_pred.shape[0]

    if best_score>score:

        wining=i
        best_score=score


test["pred_ela"]=pred_ensembler(wining,pred_dict)
test["ae"]= test[["pred_ela","Goal"]].apply(lambda x: x["pred_ela"]-x["Goal"] ,axis=1)


quantiles=test.groupby(["league.id","team_id",],as_index=False)["ae"].aggregate(["median",up_max,down_min])


#Up Coming Matches
data_dir="C:/Users/Arda/Downloads/Upwork/Portfolio/Betting_Asistant/data/"
data=pd.read_csv("{}datamart.csv".format(data_dir))
new_data= data[data["fixture.status.short"]!="FT"].reset_index(drop=True)

new_preds=pred_model(lg_model,new_data)

check_data=new_data[["fixture.id","league.id",'league.name', 
          'league.country','league.round',
           'teams.home.name', 'teams.away.name',
           'fixture_date',"team_id","Home"]]
check_data["preds"]=new_preds

check_data_v1=check_data.merge(quantiles)
check_data_v1["Lower_q"]=check_data_v1.preds+check_data_v1["down_min"]
check_data_v1["Lower_q"][check_data_v1["Lower_q"]<0]=0

check_data_v1["Upper_q"]= check_data_v1.preds+check_data_v1["up_max"]

up_comming=check_data_v1[check_data_v1.fixture_date<="2025-04-31"]
up_comming_v1=up_comming.groupby(list(up_comming.columns[:7]),as_index=False)[["preds","Lower_q","Upper_q"]].sum()

up_comming_v1["odd_up"]=(up_comming_v1["Upper_q"]-2.5)/(up_comming_v1["Upper_q"]-up_comming_v1["Lower_q"])
up_comming_v1["odd_down"]=(2.5-up_comming_v1["Lower_q"])/(up_comming_v1["Upper_q"]-up_comming_v1["Lower_q"])


up_comming_v1.to_excel("data/predicts.xlsx",index=False)

print("Prediction for upcoming matches are saved into data direction") 