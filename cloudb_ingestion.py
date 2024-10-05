# This will run after the the matchday has passed - CRON or Airflow
# The idea is to scrape the fixtures with scores and then compare the effectiveness of the model's output
# Will help also for the V3.0 model
# Using a supabase postgreSQL instance
# Pulling the data into a PowerBi dashboard for analysis

import pandas as pd
import numpy as np
import os

# Sample of last week's data (Should be official, but we'll use last week's since I ran the program today in the morning)

#df_ou = pd.read_csv('/Predictions/H2H_Predictions_Official.csv') # - uncomment when ready
#df_h2h = pd.read_csv('/Predictions/OU_Predictions_Official.csv') # - uncomment when ready

# Template with Over/Under
df_ou = pd.read_csv('Predictions/OU_Predictions_09-20-2024.csv')

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

epl_results = past_fixtures_scraper("https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures")


# Mapping for understat values to match with fbref.com fixtures
# Still need to do for other leagues but premier

mapping = {
    # La Liga: 
    #'Alaves', 'Real Valladolid', 'Osasuna', 'Valencia', 'Real Madrid', 'Getafe', 'Athletic Club', 'Villarreal', 'Rayo Vallecano', 'Real Betis',
    #'Sevilla', 'Real Sociedad', 'Las Palmas', 'Girona', 'Espanyol', 'Leganes', 'Celta Vigo', 'Barcelona', 'Atletico Madrid', 'Mallorca',

    # EPL 
    'Leicester':'Leicester City', 'Wolverhampton Wanderers':'Wolves', 'Ipswich':'Ipswich Town', 'Manchester United':'Manchester Utd', 'Nottingham Forest':"Nott'ham Forest"
    
    
    #'West Ham', 'Aston Villa', 'Fulham',  'Liverpool', 'Southampton', 'Tottenham', 'Crystal Palace', 'Brighton', 'Manchester City',
    #'Chelsea',  'Newcastle United':'Newcastle Utd', 'Everton', 'Bournemouth',  
    #'Brentford',   'Arsenal',

    # Bundesliga
    #'Augsburg', 'FC Heidenheim', 'Werder Bremen', 'Union Berlin', 'Bochum', 'Eintracht Frankfurt', 'Bayer Leverkusen', 'VfB Stuttgart', 'St. Pauli',
    #'Mainz 05', 'Freiburg', 'Bayern Munich', 'Hoffenheim', 'Holstein Kiel', 'Borussia M.Gladbach', 'Wolfsburg', 'Borussia Dortmund', 'RasenBallsport Leipzig',

    # Serie A
    #'Cagliari', 'Verona', 'Venezia', 'Juventus', 'Lecce', 'Fiorentina', 'Monza', 'Roma', 'Inter', 'Atalanta',
    #'Empoli', 'Torino', 'Genoa', 'Napoli', 'Parma Calcio 1913', 'Lazio', 'Bologna', 'Udinese', 'AC Milan', 'Como',

    # Ligue 1
    #'Nice', 'Lille', 'Rennes', 'Reims', 'Monaco', 'Brest', 'Angers', 'Montpellier', 'Lyon',
    #'Saint-Etienne', 'Strasbourg', 'Lens', 'Paris Saint Germain', 'Le Havre', 'Toulouse', 'Nantes', 'Auxerre', 'Marseille'

}

# Apply mapping:
df_ou = df_ou.replace({"home_team":mapping, "away_team":mapping})

# Mergin on home_team and away team
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



print(final_ou)

# Next steps: Merge H2H Home(%), Draw (%), Away(%) into final_ou 
# create schema and load into supabase



#### Loading into supabase section


## DB Information in Whatsapp for security reasons


'''
CREATE TABLE invoices_script_test_2 (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing primary key
'''