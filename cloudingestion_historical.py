import pandas as pd
import numpy as np
import os
from pydantic import BaseModel, condecimal, constr
from datetime import datetime
from typing import Optional
from supabase import create_client, Client
import json
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


# Concatenatating the dataframes
all_dfs = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9], ignore_index=True)

# Removing duplicates, based on specific columns
all_dfs = all_dfs.drop_duplicates(subset=['league', 'Source', 'home_team', 'away_team'])
# length before removing: 2188 x 10
# length after removing: 958 x 10 - PERFECT


# 2. Running Scraper - IDEALLY WE INCORPORATE THIS FUNCTION WITHIN THE MODULE FOLDER, keep it here for now

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
    return df


# 2. run the scraper for results and the merge
# Add columns needed for supabase ingestion (H2H related ones)
# Fixtures URL - Similar to ScraperScript
#print(fixtures_url)



#print(all_dfs)


# Running program
# python cloudingestion_historical.py