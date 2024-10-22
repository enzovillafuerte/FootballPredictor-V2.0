import pandas as pd
import numpy as np
import os
from pydantic import BaseModel, condecimal, constr
from datetime import datetime
from typing import Optional
from supabase import create_client, Client
import json
import time

#from ScraperScript import fixtures_url

# -------------- HISTORICAL LOAD --------------

# ------- Part 1 - Consolidating Needed URLs into a Dictionary

# leagues urls and stuff
fbref_urls = {
    'Eredivisie': 'https://fbref.com/en/comps/23/2023-2024/2023-2024-Eredivisie-Stats',
    'Bundesliga_2': 'https://fbref.com/en/comps/33/2-Bundesliga-Stats',
    'Jupiler': 'https://fbref.com/en/comps/37/Belgian-Pro-League-Stats',
    'Liga MX': 'https://fbref.com/en/comps/31/Liga-MX-Stats',
    'Primeira Liga': 'https://fbref.com/en/comps/32/Primeira-Liga-Stats',
    'Liga Argentina': 'https://fbref.com/en/comps/21/Primera-Division-Stats',
    'Brasileirao': 'https://fbref.com/en/comps/24/Serie-A-Stats',
    'MLS': 'https://fbref.com/en/comps/22/Major-League-Soccer-Stats',
    
    # Females
    'Premier W': 'https://fbref.com/en/comps/189/Womens-Super-League-Stats',
    'MLS W': 'https://fbref.com/en/comps/182/NWSL-Stats',
    'Spain W': 'https://fbref.com/en/comps/230/Liga-F-Stats',
    'Bundesliga W': 'https://fbref.com/en/comps/183/Frauen-Bundesliga-Stats'
}
fbref_G_urls = {
    'Peru': 'https://fbref.com/en/comps/44/Liga-1-Stats',                      # Apertura, Clausura
    'Ecuador': 'https://fbref.com/en/comps/58/Serie-A-Stats',                  # Apertura, Clausura
    'Paraguay': 'https://fbref.com/en/comps/61/Primera-Division-Stats',        # Apertura, Clausura
    'Uruguay': 'https://fbref.com/en/comps/45/Primera-Division-Stats',         # Apertura, Clausura
    'Chile': 'https://fbref.com/en/comps/35/Primera-Division-Stats',
    'Hungary': 'https://fbref.com/en/comps/46/NB-I-Stats',
    'Romania': 'https://fbref.com/en/comps/47/Liga-I-Stats',
    'Serbia': 'https://fbref.com/en/comps/54/Serbian-SuperLiga-Stats',
    'Turkey': 'https://fbref.com/en/comps/26/Super-Lig-Stats',
    'Ukraine': 'https://fbref.com/en/comps/39/Ukrainian-Premier-League-Stats',
    'Poland': 'https://fbref.com/en/comps/36/Ekstraklasa-Stats',
    'Sweden': 'https://fbref.com/en/comps/29/Allsvenskan-Stats',              # Allsvenskan - Year Calendar
    'Norway': 'https://fbref.com/en/comps/28/Eliteserien-Stats', 
    'Switzerland': 'https://fbref.com/en/comps/57/Swiss-Super-League-Stats',  # Will not work, the table index should be 3 here
    'Bulgaria': 'https://fbref.com/en/comps/67/Bulgarian-First-League-Stats',
    'Austria': 'https://fbref.com/en/comps/56/Austrian-Bundesliga-Stats',
    'Greece': 'https://fbref.com/en/comps/27/Super-League-Greece-Stats',
    'Czechia': 'https://fbref.com/en/comps/66/Czech-First-League-Stats',
    'Croatia': 'https://fbref.com/en/comps/63/Hrvatska-NL-Stats',
    'South Korea': 'https://fbref.com/en/comps/55/K-League-1-Stats',           # Year Calendar
    'Japan': 'https://fbref.com/en/comps/25/J1-League-Stats',                  # Year Calendar
    'Saudi': 'https://fbref.com/en/comps/70/Saudi-Professional-League-Stats', 
    'Denmark': 'https://fbref.com/en/comps/50/Danish-Superliga-Stats',
    
    # Females
    'Brasil W': 'https://fbref.com/en/comps/206/Serie-A1-Stats',
    'Denmark W': 'https://fbref.com/en/comps/340/Kvindeligaen-Stats',
}
# Getting fixtures url from where we will scrape
def get_fixtures_url(standings_url):
    base_url = standings_url.rsplit('/', 1)[0]
    competition_id = standings_url.split('/')[-2]
    return f"{base_url}/schedule/{competition_id}-Scores-and-Fixtures"

# Defining new dictionary
fixtures_url = {}

# Getting new urls for FBRef leagues with xG data
for country, url in fbref_urls.items():
    fixtures_url[country] = get_fixtures_url(url)
    
# Getting new urls for FBRef leagues with Non-xG data
for country, url in fbref_G_urls.items():
    fixtures_url[country] = get_fixtures_url(url)
    
# Adding manually the dictionaries for understat leagues
ud_fixtures_url = {
    'La Liga': 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures',
    'EPL': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
    'Bundesliga': 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',
    'Serie A': 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',
    'Ligue 1': 'https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures'
}

# Appending newly added dictionary to the previous one
fixtures_url.update(ud_fixtures_url)

# Exporting into json format for reuse in incremental loading
filename = 'fixtures_url.json'
with open(filename, 'w') as json_file:
    json.dump(fixtures_url, json_file, indent=4) # indent=4 for pretty formatting


# ------- Part 2 - Consolidating Historical Predictions into One Dataframe

# 1. loading datasets
df1 = pd.read_csv('Predictions/OU_Predictions_09-01-2024.csv')
df2 = pd.read_csv('Predictions/OU_Predictions_09-02-2024.csv')
df3 = pd.read_csv('Predictions/OU_Predictions_09-12-2024.csv')
df4 = pd.read_csv('Predictions/OU_Predictions_09-17-2024.csv')
df5 = pd.read_csv('Predictions/OU_Predictions_09-20-2024.csv')
df6 = pd.read_csv('Predictions/OU_Predictions_09-30-2024.csv')
df7 = pd.read_csv('Predictions/OU_Predictions_10-03-2024.csv')
df8 = pd.read_csv('Predictions/OU_Predictions_10-04-2024.csv')
df9 = pd.read_csv('Predictions/OU_Predictions_10-05-2024.csv')
# df10 = 

# Concatenatating the dataframes
all_dfs = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9], ignore_index=True)

# Removing duplicates, based on specific columns
all_dfs = all_dfs.drop_duplicates(subset=['league', 'Source', 'home_team', 'away_team'])
# length before removing: 2188 x 10
# length after removing: 958 x 10 - PERFECT

# Mapping so the names from understat match with the fixtures ones
mapping = {
    # La Liga: 
    'Alaves': 'Alavés', 'Real Valladolid': 'Valladolid', 'Real Betis':'Betis', 'Leganes':'Leganés', 
    'Atletico Madrid':'Atlético Madrid',
  
    # EPL 
    'Leicester':'Leicester City', 'Wolverhampton Wanderers':'Wolves', 'Ipswich':'Ipswich Town',
     'Manchester United':'Manchester Utd', 'Nottingham Forest':"Nott'ham Forest",
    
    # Bundesliga
    'FC Heidenheim':'Heidenheim', 'Eintracht Frankfurt':'Eint Frankfurt', 'Bayer Leverkusen':'Leverkusen', 
    'VfB Stuttgart':'Stuttgart', 'Borussia M.Gladbach':'Gladbach', 'Borussia Dortmund':'Dortmund', 
    'RasenBallsport Leipzig':'RB Leipzig',

    # Serie A
    'Verona': 'Hellas Verona', 'Parma Calcio 1913':'Parma', 'AC Milan':'Milan',

    # Ligue 1
    'Saint-Etienne':'Saint-Étienne', 'Paris Saint Germain': 'Paris S-G'

}

# Apply mapping:
all_dfs = all_dfs.replace({"home_team":mapping, "away_team":mapping})

# Adding needed columns (workaround for H2H data)
all_dfs[['home_h2h', 'draw_h2h', 'away_h2h']] = 0.00
'''
final_list of columns in order fo reference: ['league', 'Source', 'home_team', 'away_team', '+1.5(%)', '+2.5(%)',
       '+3.5(%)', 'H+1.5(%)', 'A+1.5(%)', 'xG', 'home_h2h', 'draw_h2h',
       'away_h2h']
'''

# ------- Part 3 - Running the Scraper
# IDEALLY WE INCORPORATE THIS FUNCTION WITHIN THE MODULE FOLDER, keep it here for now

# Function for scraping fixtures from fbref.com
def past_fixtures_scraper(url):
    
    df = pd.read_html(url)
    df = df[0]

    # deleting rows with null values in a specific column
    df.dropna(subset=['Wk'], inplace=True)

    # Only getting rows with Match Report (Has passed already)
    df = df[df['Match Report'] == 'Match Report']
    
    # keep only home and away columns
    df = df[['Home', 'Away', 'Score']] # Keeping xG and xG.1 out since most leagues don't have it
    df.reset_index(drop=True,inplace=True)
    
    # change name to home_team and away_team
    df.columns = ['home_team', 'away_team', 'score'] #, 'xg_h', 'xg_a']

    # add total_xG
    # df['actual_xg'] = df['xg_h'] + df['xg_a']
    
    return df


# Scrapping the data and store in new dictionary - will take a while and could be heavy
results_data = {}

for league, url in fixtures_url.items():
    key = f'{league}'
    try:
        results_data[key] = past_fixtures_scraper(url)
        print(f'Success: Results for {league}')
    except Exception as e:
        print(f'Failed to scrape {league} results: {e}')
        continue
    time.sleep(10)



# Consolidating all the data in the dictionary into one dataframe
# Beware that the key is the league, the value is a dataframe
results_consolidated = []

for league, df in results_data.items():
    df['League'] = league # adding a new column for the league
    results_consolidated.append(df)

# it creates a list with sublists
# concatenating all the DataFrames within the sublists/list into a single DataFrame
results_c_df = pd.concat(results_consolidated, ignore_index=True)

# Test before here
# ------- Part 4 - Merging Dataframes
# merge the final results dataframe with the predictions data
final_ou = pd.merge(all_dfs, results_c_df, on=['home_team', 'away_team'])

# getting rid of 'League' column as it is redundant
final_ou = final_ou.drop(columns=['League'])


# ------- Part 5 - Further Transformations & New Variables Section
# We want to: 
#           1. Get the total goals and see if +1.5 o +2.5 ratio
#           2. Get who won - Later

final_ou['g_h'] = final_ou['score'].str.split('–').str[0].astype(int)
final_ou['g_a'] = final_ou['score'].str.split('–').str[1].astype(int)
final_ou['total_goals'] = final_ou['g_h'] + final_ou['g_a']
#final_ou['R+1.5'] = final_ou['total_goals'].apply()

# Assigning values for over and under and winner
# Overall  O/U
final_ou.loc[final_ou['total_goals'] > 2.5, ['R+1.5', 'R+2.5']] = [1, 1]
final_ou.loc[(final_ou['total_goals'] > 1.5) & (final_ou['total_goals'] <= 2.5), ['R+1.5', 'R+2.5']] = [1, 0]
final_ou.loc[final_ou['total_goals'] <= 1.5, ['R+1.5', 'R+2.5']] = [0, 0]

# Ind O/U
final_ou.loc[final_ou['g_h'] > 1.5, ['RH+1.5']] = 1
final_ou.loc[final_ou['g_h'] < 1.5, ['RH+1.5']] = 0
final_ou.loc[final_ou['g_a'] > 1.5, ['RA+1.5']] = 1
final_ou.loc[final_ou['g_a'] < 1.5, ['RA+1.5']] = 0

# Overall H2H
final_ou.loc[final_ou['g_h'] > final_ou['g_a'], ['WIN']] = 'H'
final_ou.loc[final_ou['g_h'] < final_ou['g_a'], ['WIN']] = 'A'
final_ou.loc[final_ou['g_h'] == final_ou['g_a'], ['WIN']] = 'E'


# Displaying the final dataframe
print(final_ou)

# ------- Part 6 - Database Injection

# Handling in case there's null values. Changing to None just in case
final_ou = final_ou.where(pd.notnull(final_ou), None)

# Changing the column naming to lower case. Otherwise it might cause match errors with Supabase's schema definition
final_ou.columns = final_ou.columns.str.lower()

# Changing the column names appropiately to match the pydantic model and the schema generation at supabase
final_ou.rename(columns={'+1.5(%)': 'over_1_5', '+2.5(%)':'over_2_5', '+3.5(%)': 'over_3_5', 'h+1.5(%)':'home_over_1_5', 'a+1.5(%)':'away_over_1_5', 'r+1.5': 'r_1_5', 'r+2.5':'r_2_5', 'rh+1.5':'r_home_1_5', 'ra+1.5':'r_away_1_5'}, inplace=True) ## Add the H2H later

# remove duplicated records that may exist
final_ou = final_ou.drop_duplicates(subset=['league', 'source', 'home_team', 'away_team'], keep='first')

# Export into CSV
final_ou.to_csv('HistoricalLoadRecords.csv', index=False)

# Creating pydantic model. Useful for validation as the datatypes in the df must match the schema types in Supabase
class DataModel(BaseModel):
    league: str
    source: str
    home_team: str
    away_team: str
    over_1_5: float
    over_2_5: float
    over_3_5: float
    home_over_1_5: float
    away_over_1_5: float
    xg: Optional[float]  # Assuming this column could have missing values based on the subset
    home_h2h: Optional[float]  # Assuming these columns are missing in the current subset but may be present in full
    draw_h2h: Optional[float]
    away_h2h: Optional[float]
    score: str
    g_h: int
    g_a: int
    total_goals: int
    r_1_5: float
    r_2_5: float
    r_home_1_5: float
    r_away_1_5: float
    win: str


# Running the validation
for x in final_ou.to_dict(orient="records"):
    try:
        DataModel(**x).dict()
        #print('Successful Validation')

    except Exception as e:
        print(e)
        break

## DB Information in Whatsapp for security reasons
project_url = ''
api_key = ''

'''
The insertion is not working
'''
# creating supabase instance
supabase = create_client(project_url, api_key)

# Defining function for insertion
def insert_records(df, supabase):

    records = [
        DataModel(**x).model_dump()
        for x in df.to_dict(orient='records')
    ]

    # Upsert will work for inserting new rows and also update already existing ones based on primary key
    # //// Table name as argument in function better ///
    executing = supabase.table('predictions_results').upsert(records).execute() # we can also do batch, but it will not be needed in this case
    print("Successful Insertion")

# Inserting records
insert_records(final_ou, supabase)


'''
ERROR MESSAGE: Need Batch 
/Users/enzovillafuerte/Documents/GitHub/FootballPredictor-V2.0/cloudingestion_historical.py:295: PydanticDeprecatedSince20: The `dict` method is deprecated; use `model_dump` instead. Deprecated in Pydantic V2.0 to be removed in V3.0. See Pydantic V2 Migration Guide at https://errors.pydantic.dev/2.5/migration/
  DataModel(**x).dict()
2024-10-20 01:09:02,755:INFO - HTTP Request: POST https://qzyklxzvjikqnoqcjdvx.supabase.co/rest/v1/predictions_results "HTTP/1.1 400 Bad Request"
Traceback (most recent call last):
  File "/Users/enzovillafuerte/Documents/GitHub/FootballPredictor-V2.0/cloudingestion_historical.py", line 326, in <module>
    insert_records(final_ou, supabase)
  File "/Users/enzovillafuerte/Documents/GitHub/FootballPredictor-V2.0/cloudingestion_historical.py", line 322, in insert_records
    executing = supabase.table('predictions_results').upsert(records).execute() # we can also do batch, but it will not be needed in this case
  File "/Users/enzovillafuerte/opt/anaconda3/lib/python3.9/site-packages/postgrest/_sync/request_builder.py", line 70, in execute
    raise APIError(r.json())
postgrest.exceptions.APIError: {'code': 'PGRST102', 'details': None, 'hint': None, 'message': 'Empty or invalid json'}
'''
#print(all_dfs.columns)
#print(results_c_df)
#print('all good')
# Running program
# python cloudingestion_historical.py

# Everything in PowerBI
# Also add predictions there with None values for the scores 
# Using upsert so it updates itself later when ingesting into supabase.