#!/usr/bin/env python
# coding: utf-8

# # Web Scraper



# In[1]:


import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
import os
import warnings
import time
from scipy.stats import poisson
from datetime import datetime


# Functions from Module created

# Scraping Functions
from module import scraper_understat, scrape_fbref_xG, scrape_standings_mls, scrape_fbref_NonxG, fixtures_api, fixtures_scraper

# Poisson functions
from module import ud_predict_game_results, ud_predict_game_winner, fbref_predict_game_result, fbref_predict_game_winner, fbref_predict_game_result_Goals, fbref_predict_game_winner_Goals

warnings.filterwarnings('ignore')


# ## Understat.com

# In[47]:


# Defining URLs and their corresponding league names
understat_urls = {
    'La Liga': 'https://understat.com/league/La_liga/',
    'EPL': 'https://understat.com/league/EPL/',
    'Bundesliga': 'https://understat.com/league/Bundesliga/',
    'Serie A': 'https://understat.com/league/Serie_A/',
    'Ligue 1': 'https://understat.com/league/Ligue_1/',
    #'Russia': 'https://understat.com/league/RFPL/'
}

# Initializing empty dictionary to store frames
dfs_u = {}

# Defining target directory
output_dir = 'Standings'
os.makedirs(output_dir, exist_ok=True)

# For loop to o over the URLs and scrape data
for league, url in understat_urls.items():
    try:
        dfs_u[league] = scraper_understat(url)
        # Save the dataframe to a CSV file
        output_path = os.path.join(output_dir, f"{league}_standings.csv")
        dfs_u[league].to_csv(output_path, index=False)
        print(f"Successfully scraped and saved {league} data.")
    except Exception as e:
        print(f"Failed to scrape {league} data: {e}")


# ## Fbref.com - xG Data

# In[3]:


# Defining URLs and their corresponding league names
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

# Initializing empty dictionary to store frames
dfs = {}

# Defining target directory
output_dir = 'Standings'
os.makedirs(output_dir, exist_ok=True)

# For loop to o over the URLs and scrape data
for league, url in fbref_urls.items(): 
    
    if league == 'MLS':
        
        try:
            mls_eastern = scrape_fbref_xG(url)
            mls_western = scrape_standings_mls(url)
            mls_st = pd.concat([mls_eastern, mls_western], ignore_index=True)
            dfs[league] = mls_st
            # Save the dataframe to a CSV file
            output_path = os.path.join(output_dir, f"{league}_standings.csv")
            dfs[league].to_csv(output_path, index=False)
            print(f"Successfully scraped and saved {league} data.")
        except Exception as e:
            print(f"Failed to scrape {league} data: {e}")
    else:
        try:
            dfs[league] = scrape_fbref_xG(url)
            # Save the dataframe to a CSV file
            output_path = os.path.join(output_dir, f"{league}_standings.csv")
            dfs[league].to_csv(output_path, index=False)
            print(f"Successfully scraped and saved {league} data.")
        except Exception as e:
            print(f"Failed to scrape {league} data: {e}")
            
    time.sleep(5)


# ## FBRef.com - Goals

# In[4]:


# Defining URLs and their corresponding league names
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

# Initializing empty dictionary to store frames
# dfs = {}

# Defining target directory
output_dir = 'Standings'
os.makedirs(output_dir, exist_ok=True)

# For loop to o over the URLs and scrape data
for league, url in fbref_G_urls.items():
    try:
        dfs[league] = scrape_fbref_NonxG(url)
        # Save the dataframe to a CSV file
        output_path = os.path.join(output_dir, f"{league}_standings.csv")
        dfs[league].to_csv(output_path, index=False)
        print(f"Successfully scraped and saved {league} data.")
    except Exception as e:
        print(f"Failed to scrape {league} data: {e}")
        
    time.sleep(5)


# ## Fixture Scraper

# ### Football-data.org API

# In[44]:


## For understat.com data we will be leveraging the API (Except Netherlands and Russia)

# site: https://www.football-data.org/client/home
# Defining the api_key and the url where the requests will be sent
api_key = "c16a0945a1f741b8a1ac14b7246c7595"

# Different leagues url
epl_url = f"https://api.football-data.org/v2/competitions/PL/matches?status=SCHEDULED"
laliga_url = f"https://api.football-data.org/v2/competitions/PD/matches?status=SCHEDULED"
bundesliga_url = f"https://api.football-data.org/v2/competitions/BL1/matches?status=SCHEDULED"
seriea_url = f"https://api.football-data.org/v2/competitions/SA/matches?status=SCHEDULED"
ligue1_url = f"https://api.football-data.org/v2/competitions/FL1/matches?status=SCHEDULED"

# Defining headers
headers = {"X-Auth-Token": api_key}

# Extracting data
epl_fixtures = fixtures_api(epl_url, headers)
laliga_fixtures = fixtures_api(laliga_url, headers)
bundesliga_fixtures = fixtures_api(bundesliga_url, headers)
seriea_fixtures = fixtures_api(seriea_url, headers)
ligue1_fixtures = fixtures_api(ligue1_url, headers)
#russia_fixtures = get_fixtures_url('https://fbref.com/en/comps/30/Russian-Premier-League-Stats')

# Stores in memory only ~ for now


# ### Fbref.com Fixtures

# In[6]:


# Defining a function to extract pattern from standings url and transform url into fixtures url
# No need to store in Module since it is small
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
    
fixtures_url


# In[7]:


# Creating new dictionary for storing scraped data
fixtures_data = {}

# Scrapping the data and storing n new dictionary
for league, url in fixtures_url.items():
    key = f'{league} Fixture'
    try:
        fixtures_data[key] = fixtures_scraper(url)
        print(f'Success: Fixtures for {league}')
    except Exception as e:
        print(f'Failed to scrape {league} data: {e}')
        continue
    time.sleep(5)


# ##### ** Data Engineering section finalized

# In[ ]:





# ## Poisson Modeling

# In[ ]:





# In[52]:

leagues_with_14_teams =     ['Liga Argentina', 'MLS']
leagues_with_6_teams =      ['Premier W', 'Bundesliga W', 'Paraguay', 'Hungary', 'Switzerland', 'Austria', 'Croatia', 'South Korea', 
                                'Denmark', 'Denmark W']
leagues_with_7_teams =      ['MLS W', 'Greece']
leagues_with_8_teams =      ['Jupiler', 'Spain W', 'Ecuador', 'Uruguay', 'Chile', 'Romania', 'Serbia', 'Ukraine', 'Norway', 
                                'Bulgaria', 'Czechia', 'Brasil W']
leagues_with_9_teams =      ['Bundesliga', 'Ligue 1', 'Eredivisie', 'Bundesliga_2', 'Liga MX', 'Primeira Liga', 
                                'Peru', 'Turkey', 'Poland', 'Sweden', 'Saudi']

predictions_dict_ou = {}
predictions_dict_h2h = {}


# ### Understat

# In[53]:


# For loop to o over the URLs and scrape data
for league, df in dfs_u.items():
    
    # Dynamically access fixture dataframe based on league name
    df_fixtures = globals()[f'{league.lower().replace(" ","")}_fixtures']
    
    # Determine fixture records to process - in relation to the number of teams in the league
    if league in leagues_with_14_teams:
        df_matchdays = df_fixtures.loc[0:13]
    elif league in leagues_with_6_teams:
        df_matchdays = df_fixtures.loc[0:5]
    elif league in leagues_with_7_teams:
        df_matchdays = df_fixtures.loc[0:6]
    elif league in leagues_with_8_teams:
        df_matchdays = df_fixtures.loc[0:7]
    elif league in leagues_with_9_teams:
        df_matchdays = df_fixtures.loc[0:8]
    else:
        df_matchdays = df_fixtures.loc[0:9]
    
    # ******************* O/U Predictions ******************* #
    
    try:
        
        # creating an empty list to store the predictions for each game
        predictions = []

        for i, row in df_matchdays.iterrows():
            home_team = row['home_team']
            away_team = row['away_team']
            #print(f"Match {i+1}: {home_team} vs {away_team}")
            #print(" ")
            result = ud_predict_game_results(home_team, away_team, df)
            predictions.append({
                'league': league,
                'Source': 'ud',
                'home_team': home_team,
                'away_team': away_team,
                '+1.5(%)' : result['prob_over_1_goal'],
                '+2.5(%)': result['prob_over_2_goals'],
                '+3.5(%)': result['prob_over_3_goals'],
                'H+1.5(%)': result['h_+1.5'],
                'A+1.5(%)': result['a_+1.5'],
                #'AA(%)': result['AA'],
                'xG': result['expected_goals']

            })

        
        # Appending into the dictionary
        predictions_dict_ou[league] = predictions
        print(f'Sucess O/U for {league}') 
        
    except Exception as e:
        print(f"Failed to generate O/U predictions for {league} data: {e}")
        
    # ******************* H2H Predictions ******************* #
    
    try:
        
        # creating an empty list to store the predictions for each game
        predictions_h2h = []

        for i, row in df_matchdays.iterrows():
            home_team = row['home_team']
            away_team = row['away_team']


            #print(f"Match {i+1}: {home_team} vs {away_team}")
            #print(" ")
            result_h2h = ud_predict_game_winner(home_team, away_team, df)
            predictions_h2h.append({
                'League': league,
                'Source': 'ud',
                'home_team': home_team,
                'away_team': away_team,
                'Home (%)': result_h2h['home_win_prob'],
                'Draw (%)': result_h2h['draw_prob'],
                'Away (%)': result_h2h['away_win_prob'],
            })
            #print(result_h2h)
            #print(" ")

            # Appending into dictionary
            predictions_dict_h2h[league] = predictions_h2h
            print(f'Success H2H for {league}')
        
    except Exception as e:
        print(f"Failed to generate H2H predictions for {league} data: {e}")


# ### FBref xG

# In[66]:


# For loop to o over the URLs and scrape data
for league, df in dfs.items():
    
    # Dynamically access fixture dataframe based on league name
    # df_fixtures = globals()[f'{league.lower().replace(" ","")}_fixtures']
    df_fixtures = fixtures_data[f'{league} Fixture']
    
    # Determine fixture records to process - in relation to the number of teams in the league
    if league in leagues_with_14_teams:
        df_matchdays = df_fixtures.loc[0:13]
    elif league in leagues_with_6_teams:
        df_matchdays = df_fixtures.loc[0:5]
    elif league in leagues_with_7_teams:
        df_matchdays = df_fixtures.loc[0:6]
    elif league in leagues_with_8_teams:
        df_matchdays = df_fixtures.loc[0:7]
    elif league in leagues_with_9_teams:
        df_matchdays = df_fixtures.loc[0:8]
    else:
        df_matchdays = df_fixtures.loc[0:9]
    
    # ******************* O/U Predictions ******************* #
    
    # ****************** Predictions for Leagues with xG data ******************* #
    
    try:
        
        # creating an empty list to store the predictions for each game
        predictions = []

        for i, row in df_matchdays.iterrows():
            home_team = row['home_team']
            away_team = row['away_team']
            #print(f"Match {i+1}: {home_team} vs {away_team}")
            #print(" ")
            result = fbref_predict_game_result(home_team, away_team, df)
            predictions.append({
                'league': league,
                'Source': 'fbxg',
                'home_team': home_team,
                'away_team': away_team,
                '+1.5(%)' : result['prob_over_1_goal'],
                '+2.5(%)': result['prob_over_2_goals'],
                '+3.5(%)': result['prob_over_3_goals'],
                'H+1.5(%)': result['h_+1.5'],
                'A+1.5(%)': result['a_+1.5'],
                #'AA(%)': result['AA'],
                'xG': result['expected_goals']

            })

        
        # Appending into the dictionary
        predictions_dict_ou[league] = predictions
        print(f'Sucess O/U for {league}') 
        
    
        
    except Exception as e:
        
        try:
            
            # creating an empty list to store the predictions for each game
            predictions = []

            for i, row in df_matchdays.iterrows():
                home_team = row['home_team']
                away_team = row['away_team']
                #print(f"Match {i+1}: {home_team} vs {away_team}")
                #print(" ")
                result = fbref_predict_game_result_Goals(home_team, away_team, df)
                predictions.append({
                    'league': league,
                    'Source': 'fbxg_n',
                    'home_team': home_team,
                    'away_team': away_team,
                    '+1.5(%)' : result['prob_over_1_goal'],
                    '+2.5(%)': result['prob_over_2_goals'],
                    '+3.5(%)': result['prob_over_3_goals'],
                    'H+1.5(%)': result['h_+1.5'],
                    'A+1.5(%)': result['a_+1.5'],
                    #'AA(%)': result['AA'],
                    'xG': result['expected_goals']

                })


            # Appending into the dictionary
            predictions_dict_ou[league] = predictions
            print(f'Sucess O/U for {league}') 
            
        except Exception as e:
            print(f"Failed to generate O/U predictions for {league} data: {e}")
        
        
    # ******************* H2H Predictions ******************* #
    
    try:
        
        # creating an empty list to store the predictions for each game
        predictions_h2h = []

        for i, row in df_matchdays.iterrows():
            home_team = row['home_team']
            away_team = row['away_team']


            #print(f"Match {i+1}: {home_team} vs {away_team}")
            #print(" ")
            result_h2h = fbref_predict_game_winner(home_team, away_team, df)
            predictions_h2h.append({
                'League': league,
                'Source': 'fbxg',
                'home_team': home_team,
                'away_team': away_team,
                'Home (%)': result_h2h['home_win_prob'],
                'Draw (%)': result_h2h['draw_prob'],
                'Away (%)': result_h2h['away_win_prob'],
            })
            #print(result_h2h)
            #print(" ")

            # Appending into dictionary
            predictions_dict_h2h[league] = predictions_h2h
            print(f'Success H2H for {league}')
        
    except Exception as e:
        
        try:
        
            # creating an empty list to store the predictions for each game
            predictions_h2h = []

            for i, row in df_matchdays.iterrows():
                home_team = row['home_team']
                away_team = row['away_team']


                #print(f"Match {i+1}: {home_team} vs {away_team}")
                #print(" ")
                result_h2h = fbref_predict_game_winner_Goals(home_team, away_team, df)
                predictions_h2h.append({
                    'League': league,
                    'Source': 'fbxg',
                    'home_team': home_team,
                    'away_team': away_team,
                    'Home (%)': result_h2h['home_win_prob'],
                    'Draw (%)': result_h2h['draw_prob'],
                    'Away (%)': result_h2h['away_win_prob'],
                })
                #print(result_h2h)
                #print(" ")

                # Appending into dictionary
                predictions_dict_h2h[league] = predictions_h2h
                print(f'Success H2H for {league}')
                
        except Exception as e:
            print(f"Failed to generate H2H predictions for {league} data: {e}") 


# # Data Consolidation

# In[73]:


flat_data_ou = [item for sublist in predictions_dict_ou.values() for item in sublist]

# Creating DataFrame
df_ou = pd.DataFrame(flat_data_ou)

# Displaying DataFrame
df_ou


# In[72]:


flat_data_h2h = [item for sublist in predictions_dict_h2h.values() for item in sublist]

# Creating DataFrame
df_h2h = pd.DataFrame(flat_data_h2h)

# Displaying DataFrame
df_h2h


#### Saving dataframes into csv's #####
today = datetime.today()
today = today.strftime("%m-%d-%Y")

# Defining target directory
output_dir = 'Predictions'
os.makedirs(output_dir, exist_ok=True)

# Tumama
# The following is for the historical record
output_path = os.path.join(output_dir, f"OU_Predictions_{today}.csv")
df_ou.to_csv(output_path, index=False)
output_path = os.path.join(output_dir, f"H2H_Predictions_{today}.csv")
df_h2h.to_csv(output_path, index=False)

# The following is for the actual file for the automation (email or tableau)
output_path = os.path.join(output_dir, f"OU_Predictions_Official.csv")
df_ou.to_csv(output_path, index=False)
output_path = os.path.join(output_dir, f"H2H_Predictions_Official.csv")
df_h2h.to_csv(output_path, index=False)

# Uploading to github

''' 
To-Dos:
    - save the file into a csv ******* DONE *******
    - Update mapping for understat.com fixtures data
    - Update leagues link for FBREF and Understat (if applicable)
    - Count number of teams per league (so we can edit the loc section for fixtures) - ******* DONE *******
    - Plan ahead on what to do with final dataset?
        1. Stored and shared as a csv file? - THIS IS THE ONE TO GO FOR NOW
        2. Automate Tableau Dashboards (Still a bit of manual process) - LESS PRIORITY
        3. Automate sending message once it is done through office? - This one sounds cool and faster - TRY TO DO THIS ONE - https://www.youtube.com/watch?v=g_j6ILT-X0k
    - Schedule jobs using CRON for Tuesday 12am and Thursday 12am.
    - Start planning on the ideas to integrate actual odds data.
    - PDF Reports (To be decided)
    - Kelly formula (To be decided)
    - Dashboards (To be decided)
    - Some kind of descriptive analytics need to be done.- later in future
        1. Challenge: Integrate with results to see whether the prediction was accurate.
            - Potential Solution: Different program that takes last predictions and scrapes results from fixtures & scores to compare.
            - Employ Scholz for Data Entry
            - Might take a bit of time. Lower priority.
'''

'''
To-Dos for Advanced Data Science Model Project
    - Data Modeling and Schema Generation for pulling data from fbref.com consistently every week
    - Include data coming from Poisson model (?)
    - Create ETL for extracting and loading into Supabase.
    - Orchestrate weekly to have enough data.
    - Have it done for February, to start testing it for March, April and May (Hot months)
'''