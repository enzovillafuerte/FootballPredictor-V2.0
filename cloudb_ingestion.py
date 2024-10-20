# This will run after the the matchday has passed - CRON or Airflow
# The idea is to scrape the fixtures with scores and then compare the effectiveness of the model's output
# Will help also for the V3.0 model
# Using a supabase postgreSQL instance
# Pulling the data into a PowerBi dashboard for analysis

import pandas as pd
import numpy as np
import os
from pydantic import BaseModel, condecimal, constr
from datetime import datetime
from typing import Optional
from supabase import create_client, Client

# Sample of last week's data (Should be official, but we'll use last week's since I ran the program today in the morning)

#df_ou = pd.read_csv('/Predictions/H2H_Predictions_Official.csv') # - uncomment when ready
#df_h2h = pd.read_csv('/Predictions/OU_Predictions_Official.csv') # - uncomment when ready

# Template with Over/Under
df_ou = pd.read_csv('Predictions/OU_Predictions_10-07-2024.csv')
df_h2h = pd.read_csv('Predictions/H2H_Predictions_10-07-2024.csv')

# fixtures_url



# Function for scraping fixtures from fbref.com
def past_fixtures_scraper(url):
    
    df = pd.read_html(url)
    df = df[0]

    # deleting rows with null values in a specific column
    df.dropna(subset=['Wk'], inplace=True)

    # Only getting rows with Match Report (Has passed already)
    df = df[df['Match Report'] == 'Match Report']
    
    # keep only home and away columns
    df = df[['Home', 'Away', 'Score', 'xG', 'xG.1']]
    df.reset_index(drop=True,inplace=True)
    
    # change name to home_team and away_team
    df.columns = ['home_team', 'away_team', 'score', 'xg_h', 'xg_a']

    # add total_xG
    df['actual_xg'] = df['xg_h'] + df['xg_a']
    
    # retrieving only last 20 games for efficiency - Change it to 10 later
    return df.tail(20)


# change this to scrape for all leagues
epl_results = past_fixtures_scraper("https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures")


# Mapping for understat values to match with fbref.com fixtures
# Still need to do for other leagues but premier

mapping = {
    # La Liga: 
    'Alaves': 'Alavés', 'Real Valladolid': 'Valladolid', 'Real Betis':'Betis', 'Leganes':'Leganés', 'Atletico Madrid':'Atlético Madrid',
    #'Alaves', 'Real Valladolid', 'Osasuna', 'Valencia', 'Real Madrid', 'Getafe', 'Athletic Club', 'Villarreal', 'Rayo Vallecano', 'Real Betis',
    #'Sevilla', 'Real Sociedad', 'Las Palmas', 'Girona', 'Espanyol', 'Leganes', 'Celta Vigo', 'Barcelona', 'Atletico Madrid', 'Mallorca',

    # EPL 
    'Leicester':'Leicester City', 'Wolverhampton Wanderers':'Wolves', 'Ipswich':'Ipswich Town', 'Manchester United':'Manchester Utd', 'Nottingham Forest':"Nott'ham Forest",
    
    
    #'West Ham', 'Aston Villa', 'Fulham',  'Liverpool', 'Southampton', 'Tottenham', 'Crystal Palace', 'Brighton', 'Manchester City',
    #'Chelsea',  'Newcastle United':'Newcastle Utd', 'Everton', 'Bournemouth',  
    #'Brentford',   'Arsenal',

    # Bundesliga
    'FC Heidenheim':'Heidenheim', 'Eintracht Frankfurt':'Eint Frankfurt', 'Bayer Leverkusen':'Leverkusen', 'VfB Stuttgart':'Stuttgart', 
    'Borussia M.Gladbach':'Gladbach', 'Borussia Dortmund':'Dortmund', 'RasenBallsport Leipzig':'RB Leipzig',
    #'Augsburg', 'FC Heidenheim', 'Werder Bremen', 'Union Berlin', 'Bochum', 'Eintracht Frankfurt', 'Bayer Leverkusen', 'VfB Stuttgart', 'St. Pauli',
    #'Mainz 05', 'Freiburg', 'Bayern Munich', 'Hoffenheim', 'Holstein Kiel', 'Borussia M.Gladbach', 'Wolfsburg', 'Borussia Dortmund', 'RasenBallsport Leipzig',

    # Serie A
    'Verona': 'Hellas Verona', 'Parma Calcio 1913':'Parma', 'AC Milan':'Milan',

    #'Cagliari', 'Verona', 'Venezia', 'Juventus', 'Lecce', 'Fiorentina', 'Monza', 'Roma', 'Inter', 'Atalanta',
    #'Empoli', 'Torino', 'Genoa', 'Napoli', 'Parma Calcio 1913', 'Lazio', 'Bologna', 'Udinese', 'AC Milan', 'Como',

    # Ligue 1
    'Saint-Etienne':'Saint-Étienne', 'Paris Saint Germain': 'Paris S-G'
    #'Nice', 'Lille', 'Rennes', 'Reims', 'Monaco', 'Brest', 'Angers', 'Montpellier', 'Lyon',
    #'Saint-Etienne', 'Strasbourg', 'Lens', 'Paris Saint Germain', 'Le Havre', 'Toulouse', 'Nantes', 'Auxerre', 'Marseille'

}

# Apply mapping:
df_ou = df_ou.replace({"home_team":mapping, "away_team":mapping})

# keeping the needed columns
df_h2h = df_h2h[['home_team' , 'away_team' , 'Home (%)', 'Draw (%)', 'Away (%)']]
df_h2h = df_h2h.replace({"home_team":mapping, "away_team":mapping})
df_ou = pd.merge(df_ou, df_h2h, on=['home_team', 'away_team'])

# Merging on home_team and away team
final_ou = pd.merge(df_ou, epl_results, on=['home_team', 'away_team'])



#print(epl_results.head(10))
#print(df_ou.iloc[10:20])


# Further transformations & New variables section
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
#print(final_ou)

# Next steps: Merge H2H Home(%), Draw (%), Away(%) into final_ou 
# create schema and load into supabase



#### ------------ Loading into supabase section ------------

# Handling in case there's null values. Changing to None just in case
final_ou = final_ou.where(pd.notnull(final_ou), None)

# Changing the column naming to lower case. Otherwise it might cause match errors with Supabase's schema definition
final_ou.columns = final_ou.columns.str.lower()

# Changing the column names appropiately to match the pydantic model and the schema generation at supabase
final_ou.rename(columns={'+1.5(%)': 'over_1_5', '+2.5(%)':'over_2_5', '+3.5(%)': 'over_3_5', 
'h+1.5(%)':'home_over_1_5', 'a+1.5(%)':'away_over_1_5',
'home (%)':'home_h2h', 'draw (%)':'draw_h2h', 'away (%)':'away_h2h',
'r+1.5': 'r_1_5', 'r+2.5':'r_2_5', 
'rh+1.5':'r_home_1_5', 'ra+1.5':'r_away_1_5'}, inplace=True) ## Add the H2H later

# make sure all the datatypes make sense
# print(final_ou.dtypes) -- They do

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
        print('Successful Validation')

    except Exception as e:
        print(e)
        break

print(final_ou)



## DB Information in Whatsapp for security reasons
project_url = ''
api_key = ''

# creating supabase instance
supabase = create_client(project_url, api_key)

# Defining function for insertion
def insert_records(df, supabase):

    records = [
        DataModel(**x).dict()
        for x in df.to_dict(orient='records')
    ]

    # Upsert will work for inserting new rows and also update already existing ones based on primary key
    # //// Table name as argument in function better ///
    executing = supabase.table('predictions_results_o').upsert(records).execute() # we can also do batch, but it will not be needed in this case
    print("Successful Insertion")

# Inserting records
insert_records(final_ou, supabase)


# FINAL STEPS: INCLUDE ALL LEAGUES NOT ONLY PREMIER. FOLLOW METHODOLOGY FROM HISTORICAL LOAD
# python cloudb_ingestion.py
'''

CREATE TABLE predictions_results_o (
    match_id SERIAL PRIMARY KEY,  -- Auto-incrementing primary key
    league VARCHAR(20) NOT NULL,
    source VARCHAR(10) NOT NULL,
    home_team VARCHAR(30) NOT NULL,
    away_team VARCHAR(30) NOT NULL,
    over_1_5 FLOAT,
    over_2_5 FLOAT,
    over_3_5 FLOAT,
    home_over_1_5 FLOAT,
    away_over_1_5 FLOAT,
    home_h2h FLOAT,
    draw_h2h FLOAT,
    away_h2h FLOAT,
    xg FLOAT,
    score VARCHAR(10),
    --xg_h FLOAT,
    --xg_a FLOAT,
    --actual_xg FLOAT
    g_h INT,
    g_a INT,
    total_goals INT,
    r_1_5 FLOAT,
    r_2_5 FLOAT,
    r_home_1_5 FLOAT,
    r_away_1_5 FLOAT,
    win VARCHAR(5),
    ingestion_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- Automatically populating the 
)
'''