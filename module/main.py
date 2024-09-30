import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
import os



# *********************************************************************************************#
#************************************* SCRAPERS SECTION ***************************************#
# *********************************************************************************************#

# Function for Scraping data from understat
def scraper_understat(base_url):
    ###### Sending the request to the Web Server #######
    url = base_url
    
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'lxml')
    scripts = soup.find_all('script')

    strings = scripts[2].string

    ind_start = strings.index("('")+2 # 30, since it is the number of characters from the start of the line to the start of the json data
    ind_end = strings.index("')")
    json_data = strings[ind_start:ind_end]

    json_data = json_data.encode('utf8').decode('unicode_escape')

    #convert string to json format
    data = json.loads(json_data)
    
    ##################### Overall Standings #######################
    team_statistics = {}

    # loop though the outer dictionary (teams)
    for team_id, team_data in data.items():
        team_stats = {
            'matches': 0.0,
            'wins': 0.0,
            'draws': 0.0,
            'losses': 0.0,
            'goals': 0.0,
            'goals_against': 0.0,
            'points': 0.0,
            'xG': 0.0,
            'xGA': 0.0,
            'xPTS': 0.0
        }

        # loop through each game within the same team
        for game in team_data['history']:
            team_stats['matches'] += 1
            team_stats['wins'] += game['wins']
            team_stats['draws'] += game['draws']
            team_stats['losses'] += game['loses']
            team_stats['goals'] += game['scored']
            team_stats['goals_against'] += game['missed']
            team_stats['points'] += game['pts']
            team_stats['xG'] += game['xG']
            team_stats['xGA'] += game['xGA']
            team_stats['xPTS'] += game['xpts']


        # store the team statistics in the dictionary
        team_statistics[team_data['title']] = team_stats
        
        
        # Rename columns
        # This is overall STANDING
        # Still need to do Home and AWAY
        columns = {'matches':'M', 'wins':'W', 'draws':'D', 
                   'losses':'L', 'goals':'G', 'goals_against':'GA', 
                   'points':'PTS'}#, 'xG'xG', 'xGA', 'xPTS'}

        overall_df = pd.DataFrame(team_statistics).T # transpose
        overall_df = overall_df.rename(columns=columns)
        overall_df.reset_index(inplace=True)
        overall_df.rename(columns={'index':'Team'}, inplace=True)
        overall_df.sort_values(by='PTS', ascending=False, inplace=True)
        overall_df.reset_index(inplace=True, drop=True)
        
        
    ##################### Home Standings #######################
    
    team_stats = {}

    # loop the outer dictionary (teams)
    for team_id, team_data in data.items():
        team_stats_h = {
            'matches': 0.0,
            'wins': 0.0,
            'draws': 0.0,
            'losses': 0.0,
            'goals': 0.0,
            'goals_against': 0.0,
            'points': 0.0,
            'xG': 0.0,
            'xGA': 0.0,
            'xPTS': 0.0
        }

        # loop through each game within the same team
        for game in team_data['history']:
            if game['h_a'] == 'h':
                team_stats_h['matches'] += 1
                team_stats_h['wins'] += game['wins']
                team_stats_h['draws'] += game['draws']
                team_stats_h['losses'] += game['loses']
                team_stats_h['goals'] += game['scored']
                team_stats_h['goals_against'] += game['missed']
                team_stats_h['points'] += game['pts']
                team_stats_h['xG'] += game['xG']
                team_stats_h['xGA'] += game['xGA']
                team_stats_h['xPTS'] += game['xpts']


        # store the team statistics in the dictionary
        team_stats[team_data['title']] = team_stats_h

    home_df = pd.DataFrame(team_stats).T # transpose

    columns = {'matches':'M', 'wins':'W', 'draws':'D', 
               'losses':'L', 'goals':'G', 'goals_against':'GA', 
               'points':'PTS'}#, 'xG'xG', 'xGA', 'xPTS'}


    home_df = home_df.rename(columns=columns)
    home_df.reset_index(inplace=True)
    home_df.rename(columns={'index':'Team'}, inplace=True)
    
    
    
    ##################### Away Standings #######################
    
    team_stats = {}

    # loop the outer dictionary (teams)
    for team_id, team_data in data.items():
        team_stats_h = {
            'matches': 0.0,
            'wins': 0.0,
            'draws': 0.0,
            'losses': 0.0,
            'goals': 0.0,
            'goals_against': 0.0,
            'points': 0.0,
            'xG': 0.0,
            'xGA': 0.0,
            'xPTS': 0.0
        }

        # loop through each game within the same team
        for game in team_data['history']:
            if game['h_a'] == 'a':
                team_stats_h['matches'] += 1
                team_stats_h['wins'] += game['wins']
                team_stats_h['draws'] += game['draws']
                team_stats_h['losses'] += game['loses']
                team_stats_h['goals'] += game['scored']
                team_stats_h['goals_against'] += game['missed']
                team_stats_h['points'] += game['pts']
                team_stats_h['xG'] += game['xG']
                team_stats_h['xGA'] += game['xGA']
                team_stats_h['xPTS'] += game['xpts']


        # store the team statistics in the dictionary
        team_stats[team_data['title']] = team_stats_h

    away_df = pd.DataFrame(team_stats).T # transpose

    columns = {'matches':'M', 'wins':'W', 'draws':'D', 
               'losses':'L', 'goals':'G', 'goals_against':'GA', 
               'points':'PTS'}#, 'xG'xG', 'xGA', 'xPTS'}


    away_df = away_df.rename(columns=columns)
    away_df.reset_index(inplace=True)
    away_df.rename(columns={'index':'Team'}, inplace=True)
    
    ###################### Adding Ratios ################
    list_metric = ["xG", "xGA", "xPTS"]
    list_naming = ["xG per Game", "xGA per Game", "xPTS per Game"]
    
    # ----------------------  (overall)
    counter = 0
    while counter < len(list_metric):
        overall_df[list_naming[counter]] = overall_df[list_metric[counter]] / overall_df["M"]
        counter += 1
        
    # adding performance difference  
    for i in range(len(overall_df)):
        xG_per_game_diff = overall_df.loc[i, "G"] / overall_df.loc[i, "M"] - overall_df.loc[i, "xG per Game"]
        xGA_per_game_diff = overall_df.loc[i, "xGA per Game"] -  overall_df.loc[i, "GA"] / overall_df.loc[i, "M"]  
        overall_df.loc[i, "xG per Game Diff"] = xG_per_game_diff
        overall_df.loc[i, "xGA per Game Diff"] = xGA_per_game_diff
        
    # ----------------------  (home)
    counter = 0
    while counter < len(list_metric):
        home_df[list_naming[counter]] = home_df[list_metric[counter]] / home_df["M"]
        counter += 1
    
    for i in range(len(home_df)):
        xG_per_game_diff = home_df.loc[i, "G"] / home_df.loc[i, "M"] - home_df.loc[i, "xG per Game"]
        xGA_per_game_diff = home_df.loc[i, "xGA per Game"] -  home_df.loc[i, "GA"] / home_df.loc[i, "M"]  
        home_df.loc[i, "xG per Game Diff"] = xG_per_game_diff
        home_df.loc[i, "xGA per Game Diff"] = xGA_per_game_diff
        
    # ----------------------  (away)    
    counter = 0
    while counter < len(list_metric):
        away_df[list_naming[counter]] = away_df[list_metric[counter]] / away_df["M"]
        counter += 1
        
    for i in range(len(away_df)):
        xG_per_game_diff = away_df.loc[i, "G"] / away_df.loc[i, "M"] - away_df.loc[i, "xG per Game"]
        xGA_per_game_diff = away_df.loc[i, "xGA per Game"] -  away_df.loc[i, "GA"] / away_df.loc[i, "M"]  
        away_df.loc[i, "xG per Game Diff"] = xG_per_game_diff
        away_df.loc[i, "xGA per Game Diff"] = xGA_per_game_diff
    

    ###################### Merging the Dataframes ################
      
    # Set display options
    pd.set_option("display.max_rows", None)  # Display all rows
    pd.set_option("display.max_columns", None)  # Display all columns
    pd.set_option("display.width", None)  # Set display width to auto
        
    df_merged = pd.merge(home_df, away_df, on='Team')
    df_merged = pd.merge(overall_df, df_merged, on='Team')
    
    return df_merged

# Function for Scraping data from FBREF for Leagues with xG Data and Home/Away Standings
def scrape_fbref_xG(url):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('table')
    rows = tables[1].find_all('tr')

    table_data = []

    for row in rows:
        cols = row.find_all('td')
        table_data.append([col.get_text(strip=True) for col in cols])
        
    df = pd.DataFrame(table_data)

    df.columns = ['Squad', 'H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 'H_GA', 'H_GD', 'H_Pts', 'H_Pts/MP', 
          'H_xG', 'H_xGA', 'H_xGD', 'H_xGD/90', 'A_MP', 'A_W', 'A_D' , 'A_L', 'A_GF', 'A_GA', 'A_GD', 'A_Pts', 'A_Pts/MP',
          'A_xG', 'A_xGA', 'A_xGD', 'A_xGD/90']
    
    df = df.dropna()
    
    # change the dtypes to float
    column_list = ['H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 'H_GA', 'H_GD', 'H_Pts',
       'H_Pts/MP', 'H_xG', 'H_xGA', 'H_xGD', 'H_xGD/90', 'A_MP', 'A_W', 'A_D',
       'A_L', 'A_GF', 'A_GA', 'A_GD', 'A_Pts', 'A_Pts/MP', 'A_xG', 'A_xGA',
       'A_xGD', 'A_xGD/90']
    df[column_list] = df[column_list].astype(float)
    
    # split the df into two datasets: HOME and away
    df_home = df.iloc[:, :-13]
    df_away = df.iloc[:, 14:]
    df_away['Squad'] = df_home['Squad']
    
    #use pop method to re-arrange columns(we need the Squad to be first isntead of last)
    squad_col = df_away.pop('Squad')
    df_away.insert(0, 'Squad', squad_col)
    
    # ADDING THE RATIOS
    df_home['H_xG per Game'] = df_home['H_xG'] / df_home['H_MP']
    df_home['H_xGA per Game'] = df_home['H_xGA'] / df_home['H_MP']
    
    df_home['H_xG per Game Diff'] = df_home['H_GF'] / df_home['H_MP'] - df_home['H_xG per Game']
    df_home['H_xGA per Game Diff'] = df_home['H_xGA per Game'] -  df_home['H_GA'] / df_home['H_MP']
    
    
    
    df_away['A_xG per Game'] = df_away['A_xG'] / df_away['A_MP']
    df_away['A_xGA per Game'] = df_away['A_xGA'] / df_away['A_MP']
    
    df_away['A_xG per Game Diff'] = df_away['A_GF'] / df_away['A_MP'] - df_away['A_xG per Game']
    df_away['A_xGA per Game Diff'] = df_away['A_xGA per Game'] - df_away['A_GA'] / df_away['A_MP']
    
    
    ###### Mergin the Dataframes #########
    
    df_merged = pd.merge(df_home, df_away, on='Squad')
    
    df_merged.columns = ['Team', 'MP_x', 'W_x', 'D_x',
       'L_x', 'GF_x', 'GA_x', 'GD_x', 'Pts_x', 'Pts/MP_x', 'xG_x', 'xGA_x',
       'xGD_x', 'xGD/90_x', 'xG per Game_x', 'xGA per Game_x',
       'xG per Game Diff_x', 'xGA per Game Diff_x', 'MP_y', 'W_y', 'D_y',
       'L_y', 'GF_y', 'GA_y', 'GD_y', 'Pts_y', 'Pts/MP_y', 'xG_y', 'xGA_y',
       'xGD_y', 'xGD/90_y', 'xG per Game_y', 'xGA per Game_y',
       'xG per Game Diff_y', 'xGA per Game Diff_y']
    
    return df_merged

# scraping functions with leagues with more than 1 conference such as MLS
def scrape_standings_mls(url):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('table')
    rows = tables[3].find_all('tr')

    table_data = []

    for row in rows:
        cols = row.find_all('td')
        table_data.append([col.get_text(strip=True) for col in cols])
        
    df = pd.DataFrame(table_data)

    df.columns = ['Squad', 'H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 'H_GA', 'H_GD', 'H_Pts', 'H_Pts/MP', 
          'H_xG', 'H_xGA', 'H_xGD', 'H_xGD/90', 'A_MP', 'A_W', 'A_D' , 'A_L', 'A_GF', 'A_GA', 'A_GD', 'A_Pts', 'A_Pts/MP',
          'A_xG', 'A_xGA', 'A_xGD', 'A_xGD/90']
    
    df = df.dropna()
    
    # change the dtypes to float
    column_list = ['H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 'H_GA', 'H_GD', 'H_Pts',
       'H_Pts/MP', 'H_xG', 'H_xGA', 'H_xGD', 'H_xGD/90', 'A_MP', 'A_W', 'A_D',
       'A_L', 'A_GF', 'A_GA', 'A_GD', 'A_Pts', 'A_Pts/MP', 'A_xG', 'A_xGA',
       'A_xGD', 'A_xGD/90']
    df[column_list] = df[column_list].astype(float)
    
    # split the df into two datasets: HOME and away
    df_home = df.iloc[:, :-13]
    df_away = df.iloc[:, 14:]
    df_away['Squad'] = df_home['Squad']
    
    #use pop method to re-arrange columns(we need the Squad to be first isntead of last)
    squad_col = df_away.pop('Squad')
    df_away.insert(0, 'Squad', squad_col)
    
    # ADDING THE RATIOS
    df_home['H_xG per Game'] = df_home['H_xG'] / df_home['H_MP']
    df_home['H_xGA per Game'] = df_home['H_xGA'] / df_home['H_MP']
    
    df_home['H_xG per Game Diff'] = df_home['H_GF'] / df_home['H_MP'] - df_home['H_xG per Game']
    df_home['H_xGA per Game Diff'] = df_home['H_xGA per Game'] -  df_home['H_GA'] / df_home['H_MP']
    
    
    df_away['A_xG per Game'] = df_away['A_xG'] / df_away['A_MP']
    df_away['A_xGA per Game'] = df_away['A_xGA'] / df_away['A_MP']
    
    df_away['A_xG per Game Diff'] = df_away['A_GF'] / df_away['A_MP'] - df_away['A_xG per Game']
    df_away['A_xGA per Game Diff'] = df_away['A_xGA per Game'] - df_away['A_GA'] / df_away['A_MP']
    
    ###### Mergin the Dataframes #########
    
    df_merged = pd.merge(df_home, df_away, on='Squad')
    
    df_merged.columns = ['Team', 'MP_x', 'W_x', 'D_x',
       'L_x', 'GF_x', 'GA_x', 'GD_x', 'Pts_x', 'Pts/MP_x', 'xG_x', 'xGA_x',
       'xGD_x', 'xGD/90_x', 'xG per Game_x', 'xGA per Game_x',
       'xG per Game Diff_x', 'xGA per Game Diff_x', 'MP_y', 'W_y', 'D_y',
       'L_y', 'GF_y', 'GA_y', 'GD_y', 'Pts_y', 'Pts/MP_y', 'xG_y', 'xGA_y',
       'xGD_y', 'xGD/90_y', 'xG per Game_y', 'xGA per Game_y',
       'xG per Game Diff_y', 'xGA per Game Diff_y']
    
    return df_merged

# Scraping function for leagues with no-xG data
def scrape_fbref_NonxG(url):
    
    # scrapping by filtering on the table index number [1] (Home-Away)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('table')
    rows = tables[1].find_all('tr')
    
    table_data = []
    
    for row in rows:
        cols = row.find_all('td')
        table_data.append([col.get_text(strip=True) for col in cols])
        
    df = pd.DataFrame(table_data)
    
    # re defining the names of the columns
    df.columns = ['Squad', 'H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 
                  'H_GA', 'H_GD', 'H_Pts', 'H_Pts/MP',
                  'A_MP', 'A_W', 'A_D' , 'A_L', 'A_GF', 
                  'A_GA', 'A_GD', 'A_Pts', 'A_Pts/MP'
    ]
    
    # dropping nan values
    df = df.dropna()
    
    # change dtypes of variables to float
    column_list = ['H_MP', 'H_W', 'H_D', 'H_L', 'H_GF', 'H_GA', 'H_GD', 'H_Pts',
       'H_Pts/MP', 'A_MP', 'A_W', 'A_D', 'A_L', 'A_GF', 'A_GA', 
                   'A_GD', 'A_Pts', 'A_Pts/MP']
    
    df[column_list] = df[column_list].astype(float)
    
    # split the df into two datasets: HOME and AWAY
    df_home = df.iloc[:, :-9]
    df_away = df.iloc[:, 10:]
    df_away['Squad'] = df_home['Squad']
    
    #use pop method to re-arrange columns(we need the Squad to be first isntead of last)
    squad_col = df_away.pop('Squad')
    df_away.insert(0, 'Squad', squad_col)
    
    # ADDING RATIOS
    df_home['H_GF per Game'] = df_home['H_GF'] / df_home['H_MP']
    df_home['H_GA per Game'] = df_home['H_GA'] / df_home['H_MP']
    
    
    df_away['A_GF per Game'] = df_away['A_GF'] / df_away['A_MP']
    df_away['A_GA per Game'] = df_away['A_GA'] / df_away['A_MP']
    
    
    
    ###### Merging the Dataframes #########
    
    df_merged = pd.merge(df_home, df_away, on='Squad')
    
    df_merged.columns = ['Team', 'MP_x', 'W_x', 'D_x',
       'L_x', 'GF_x', 'GA_x', 'GD_x', 'Pts_x', 'Pts/MP_x',
       'H_GF per Game_x', 'H_GA per Game_x', 'MP_y', 'W_y', 'D_y',
       'L_y', 'GF_y', 'GA_y', 'GD_y', 'Pts_y', 'Pts/MP_y',
       'A_GF per Game_y', 'A_GA per Game_y']
    
    return df_merged

# Requesting data from API for 5 TOP Leagues
# Create function for extracting data from API - Incorporate into Module Later
def fixtures_api(url, headers):

    
    try: 

        # creating requests connection
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise ValueError(f"Failed to retrieve data: {response.content}")

        data = response.json()
        fixtures = data["matches"]    

        # create df for storing the output
        df = pd.DataFrame(columns=["home_team", "away_team"])

        for fixture in fixtures:
            home_team = fixture["homeTeam"]["name"]
            away_team = fixture["awayTeam"]["name"]
            df = df.append({"home_team": home_team, "away_team": away_team}, ignore_index=True)
            
        ''' Big Mapping Section '''
        
        mapping = {
            
            # Premier League Section 
            
            "Aston Villa FC":"Aston Villa", "Wolverhampton Wanderers FC": "Wolverhampton Wanderers", "Arsenal FC":"Arsenal", "Manchester City FC":"Manchester City",
            "Manchester United FC": "Manchester United", "Tottenham Hotspur FC" : "Tottenham", "Newcastle United FC":"Newcastle United", "Liverpool FC":"Liverpool",
            "Brighton & Hove Albion FC":"Brighton", "Brentford FC":"Brentford", "Fulham FC":"Fulham", "Chelsea FC":"Chelsea", "Crystal Palace FC":"Crystal Palace",
            "Nottingham Forest FC":"Nottingham Forest", "Everton FC": "Everton", "Leicester City FC":"Leicester", "West Ham United FC":"West Ham", "AFC Bournemouth": "Bournemouth",
            "Leeds United FC":"Leeds", "Southampton FC":"Southampton", "Luton Town FC":"Luton", "Burnley FC": "Burnley", "Sheffield United FC":"Sheffield United",
            "Ipswich Town FC": 'Ipswich',
            
            # La Liga Section
            "UD Almería":"Almeria", "Cádiz CF":"Cadiz", "Rayo Vallecano de Madrid": "Rayo Vallecano", "Girona FC":"Girona", "RCD Espanyol de Barcelona": "Espanyol",
            "RC Celta de Vigo":"Celta Vigo", "Club Atlético de Madrid":"Atletico Madrid", "Valencia CF":"Valencia", "Real Betis Balompié": "Real Betis",
            "RCD Mallorca": "Mallorca","Real Sociedad de Fútbol":"Real Sociedad", "Elche CF": "Elche", "CA Osasuna":"Osasuna", "Villarreal CF":"Villarreal",
            "Getafe CF":"Getafe", "Sevilla FC":"Sevilla", "FC Barcelona":"Barcelona", "Real Madrid CF":"Real Madrid", "Athletic Club":"Athletic Club", "Real Valladolid CF":  "Real Valladolid",
            "Deportivo Alavés":"Alaves", "Granada CF":"Granada", "UD Las Palmas":"Las Palmas", "CD Leganés": 'Leganes',
            
            # Bundesliga
            "VfB Stuttgart":"VfB Stuttgart", "VfL Wolfsburg":"Wolfsburg", "FC Augsburg":"Augsburg", "FC Schalke 04":"Schalke 04", "VfL Bochum 1848":"Bochum", 
            "RB Leipzig":"RasenBallsport Leipzig", "TSG 1899 Hoffenheim":"Hoffenheim", "Hertha BSC":"Hertha Berlin", "Borussia Dortmund":"Borussia Dortmund",
            "1. FC Köln":"FC Cologne", "1. FC Union Berlin":"Union Berlin", "Eintracht Frankfurt":"Eintracht Frankfurt", "Bayer 04 Leverkusen":"Bayer Leverkusen",
            "FC Bayern München":"Bayern Munich", "1. FSV Mainz 05":"Mainz 05", "SC Freiburg":"Freiburg", "Borussia Mönchengladbach":"Borussia M.Gladbach",
            "SV Werder Bremen":"Werder Bremen", "SV Darmstadt 98":"Darmstadt", "1. FC Heidenheim 1846":"FC Heidenheim", 
            "Holstein Kiel": 'Holstein Kiel', "FC St. Pauli 1910": 'St. Pauli', 
            
            # Serie A
            "AC Monza": "Monza", "US Cremonese":"Cremonese", "US Salernitana 1919":"Salernitana", "Bologna FC 1909":"Bologna", "Udinese Calcio":"Udinese",
            "AC Milan":"AC Milan", "UC Sampdoria":"Sampdoria","Hellas Verona FC":"Verona", "Torino FC":"Torino", "SSC Napoli":"Napoli", "ACF Fiorentina":"Fiorentina",
            "US Lecce":"Lecce", "SS Lazio":"Lazio", "AS Roma":"Roma", "FC Internazionale Milano":"Inter", "Juventus FC":"Juventus", "Atalanta BC":"Atalanta", 
            "Empoli FC": "Empoli", "US Sassuolo Calcio":"Sassuolo", "Spezia Calcio":"Spezia", "Frosinone Calcio":"Frosinone", "Genoa CFC":"Genoa", "Cagliari Calcio":"Cagliari",
            "Parma Calcio 1913": 'Parma Calcio 1913', "Venezia FC": 'Venezia', "Como 1907": 'Como',
            
            # Ligue 1
            "Toulouse FC":"Toulouse", "Olympique Lyonnais":"Lyon", "Stade Rennais FC 1901":"Rennes", "Stade de Reims": "Reims",
            "Paris Saint-Germain FC":"Paris Saint Germain", "Racing Club de Lens":"Lens", "Lille OSC": "Lille", "Montpellier HSC":"Montpellier",
            "AJ Auxerre":"Auxerre", "FC Nantes":"Nantes", "Stade Brestois 29":"Brest", "OGC Nice":"Nice", "RC Strasbourg Alsace": "Strasbourg", 
            "AC Ajaccio":"Ajaccio", "Clermont Foot 63":"Clermont Foot", "Angers SCO":"Angers", "AS Monaco FC":"Monaco", "FC Lorient":"Lorient", 
            "Olympique de Marseille":"Marseille", "ES Troyes AC":"Troyes", "Le Havre AC":"Le Havre", "FC Metz":"Metz",
            "AS Saint-Étienne": 'Saint-Etienne'    
        
        }

        df = df.replace({"home_team":mapping, "away_team":mapping})

        print(f'Sucessfully extracted data for {url}')

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from {url}: {e}")
        return None
    
    except ValueError as e:
        print(f"ValueError: {e}")
        return None
    
    except KeyError as e:
        print(f"KeyError: Missing expected data in the response: {e}")
        return None
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

    
        
    return df

# Function for scraping fixtures from fbref.com
def fixtures_scraper(url):
    
    df = pd.read_html(url)

    df = df[0]

    # deleting rows with null values in a specific column
    df.dropna(subset=['Wk'], inplace=True)

    # Only getting rows with Head-to-head (hasn't happened yet
    df = df[df['Match Report'] == 'Head-to-Head']
    
    # keep only home and away columns
    df = df[['Home', 'Away']]
    df.reset_index(drop=True,inplace=True)
    
    # change name to home_team and away_team
    df.columns = ['home_team', 'away_team']
    
    return df


# *********************************************************************************************#
#************************************* PREDICTIONS SECTION ***************************************#
# *********************************************************************************************#

#-------------------------- Understat Poisson  ----------------------------#

# Function for prediciting Over/Under
def ud_predict_game_results(home_team, away_team, df_merged):
    
    from scipy.stats import poisson

    # getting home team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['xG per Game_x', 'xGA per Game_x', 'xG per Game Diff_x', 'xGA per Game Diff_x']]
    home_xG = home_stats['xG per Game_x'].values[0]
    home_xGA = home_stats['xGA per Game_x'].values[0]
    home_xG_diff = home_stats['xG per Game Diff_x'].values[0]
    home_xGA_diff = home_stats['xGA per Game Diff_x'].values[0]

    # getting away team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['xG per Game_y', 'xGA per Game_y', 'xG per Game Diff_y', 'xGA per Game Diff_y']]
    away_xG = away_stats['xG per Game_y'].values[0]
    away_xGA = away_stats['xGA per Game_y'].values[0]
    away_xG_diff = away_stats['xG per Game Diff_y'].values[0]
    away_xGA_diff = away_stats['xGA per Game Diff_y'].values[0]

    # calculating lambda for each team (explanation in One note)
    lambda_home = (home_xG + home_xG_diff) * (away_xGA - away_xGA_diff)
    lambda_away = (away_xG + away_xG_diff) * (home_xGA - home_xGA_diff)

    # calculating the probability of the home team scoring x goals at home and the away team conceding x goals away
    prob = 0
    for i in range(3):
        for j in range(3):
            prob += poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
    '''        
    max_score=4
    for n in range(1):
        print(f"Simulation {n+1}:")
        for i in range(max_score+1):
            for j in range(max_score+1):
                score_prob = poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
                print(f"{home_team} {i} - {j} {away_team} ({score_prob:.3f})")  
                '''

    # calculating the probability of the game having over 2 goals
    prob_over_2 = 1 - poisson.cdf(2, lambda_home) * poisson.cdf(2, lambda_away)
    prob_over_1 = 1 - poisson.cdf(1, lambda_home) * poisson.cdf(1, lambda_away)
    prob_over_3 = 1 - poisson.cdf(3, lambda_home) * poisson.cdf(3, lambda_away)
    
    # calculating probability of each team having +1.5 goals
    prob_over_2_home = 1 - poisson.cdf(2, lambda_home)
    prob_over_2_away = 1 - poisson.cdf(2, lambda_away)
    
    # calculating probability of both teams scoring
    #goal_home = poisson.pmf(0, lambda_home)
    #goal_away = poisson.pmf(0, lambda_away)
    #both_score = 1 - (goal_home * goal_away)

    #calculate total xG
    lambda_total = lambda_home + lambda_away
    

    return {"home_team": home_team, "away_team": away_team, "lambda_home": lambda_home, "lambda_away": lambda_away, 
            "prob_over_2_goals": prob_over_2, "prob_over_1_goal": prob_over_1, "prob_over_3_goals": prob_over_3, 
            "h_+1.5": prob_over_2_home, "a_+1.5": prob_over_2_away, "expected_goals": lambda_total}

# Function for predicting H2H
def ud_predict_game_winner(home_team, away_team, df_merged):
    from scipy.stats import poisson
    
    # getting home team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['xG per Game_x', 'xGA per Game_x', 'xG per Game Diff_x', 'xGA per Game Diff_x']]
    home_xG = home_stats['xG per Game_x'].values[0]
    home_xGA = home_stats['xGA per Game_x'].values[0]
    home_xG_diff = home_stats['xG per Game Diff_x'].values[0]
    home_xGA_diff = home_stats['xGA per Game Diff_x'].values[0]

    # getting away team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['xG per Game_y', 'xGA per Game_y', 'xG per Game Diff_y', 'xGA per Game Diff_y']]
    away_xG = away_stats['xG per Game_y'].values[0]
    away_xGA = away_stats['xGA per Game_y'].values[0]
    away_xG_diff = away_stats['xG per Game Diff_y'].values[0]
    away_xGA_diff = away_stats['xGA per Game Diff_y'].values[0]

    # calculating lambda for home team scoring at home and away team conceding away
    lambda_home = (home_xG + home_xG_diff) * (away_xGA - away_xGA_diff)
    lambda_away = (away_xG + away_xG_diff) * (home_xGA - home_xGA_diff)

    # calculate the probability of the home team scoring x goals at home and the away team conceding x goals away
    home_probs = poisson.pmf(range(8), lambda_home)
    away_probs = poisson.pmf(range(8), lambda_away)

    # calculate probabilities of different scorlines
    score_prob = np.outer(home_probs, away_probs)
    max_score = score_prob.shape[0] - 1

    # calculating probabilities of each outcome (win, draw, lose)
    home_win_prob = np.sum(np.tril(score_prob, -1))
    draw_prob = np.sum(np.diag(score_prob))
    away_win_prob = np.sum(np.triu(score_prob, 1))
    
    # calculate the total sum of probabilities
    total_prob = home_win_prob + draw_prob + away_win_prob

    # normalize probabilities
    home_win_prob /= total_prob
    draw_prob /= total_prob
    away_win_prob /= total_prob
    
    # return results as a dictionary
    return {
        'home_team': home_team,
        'away_team': away_team,
        'home_win_prob': home_win_prob,
        'draw_prob': draw_prob,
        'away_win_prob': away_win_prob
    }


#-------------------------- FBRef.com Poisson  ----------------------------#

# Function for predicting Over/Under - xG
def fbref_predict_game_result(home_team, away_team, df_merged):
    from scipy.stats import poisson

    # getting home team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['xG per Game_x', 'xGA per Game_x', 'xG per Game Diff_x', 'xGA per Game Diff_x']]
    home_xG = home_stats['xG per Game_x'].values[0]
    home_xGA = home_stats['xGA per Game_x'].values[0]
    home_xG_diff = home_stats['xG per Game Diff_x'].values[0]
    home_xGA_diff = home_stats['xGA per Game Diff_x'].values[0]

    # getting away team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['xG per Game_y', 'xGA per Game_y', 'xG per Game Diff_y', 'xGA per Game Diff_y']]
    away_xG = away_stats['xG per Game_y'].values[0]
    away_xGA = away_stats['xGA per Game_y'].values[0]
    away_xG_diff = away_stats['xG per Game Diff_y'].values[0]
    away_xGA_diff = away_stats['xGA per Game Diff_y'].values[0]

    # calculating lambda for each team (explanation in One note)
    lambda_home = (home_xG + home_xG_diff) * (away_xGA - away_xGA_diff)
    lambda_away = (away_xG + away_xG_diff) * (home_xGA - home_xGA_diff)

    # calculating the probability of the home team scoring x goals at home and the away team conceding x goals away
    prob = 0
    for i in range(3):
        for j in range(3):
            prob += poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
    '''        
    max_score=4
    for n in range(1):
        print(f"Simulation {n+1}:")
        for i in range(max_score+1):
            for j in range(max_score+1):
                score_prob = poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
                print(f"{home_team} {i} - {j} {away_team} ({score_prob:.3f})")  
                '''

    # calculating the probability of the game having over 2 goals
    prob_over_2 = 1 - poisson.cdf(2, lambda_home) * poisson.cdf(2, lambda_away)
    prob_over_1 = 1 - poisson.cdf(1, lambda_home) * poisson.cdf(1, lambda_away)
    prob_over_3 = 1 - poisson.cdf(3, lambda_home) * poisson.cdf(3, lambda_away)
    
    # calculating probability of each team having +1.5 goals
    prob_over_2_home = 1 - poisson.cdf(2, lambda_home)
    prob_over_2_away = 1 - poisson.cdf(2, lambda_away)
    

    #calculate total xG
    lambda_total = lambda_home + lambda_away
    

    return {"home_team": home_team, "away_team": away_team, "lambda_home": lambda_home, "lambda_away": lambda_away, 
            "prob_over_2_goals": prob_over_2, "prob_over_1_goal": prob_over_1, "prob_over_3_goals": prob_over_3, 
            "h_+1.5": prob_over_2_home, "a_+1.5": prob_over_2_away,  "expected_goals": lambda_total}

# Function for predicting H2H - xG
def fbref_predict_game_winner(home_team, away_team, df_merged):
    from scipy.stats import poisson
    
    # getting home team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['xG per Game_x', 'xGA per Game_x', 'xG per Game Diff_x', 'xGA per Game Diff_x']]
    home_xG = home_stats['xG per Game_x'].values[0]
    home_xGA = home_stats['xGA per Game_x'].values[0]
    home_xG_diff = home_stats['xG per Game Diff_x'].values[0]
    home_xGA_diff = home_stats['xGA per Game Diff_x'].values[0]

    # getting away team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['xG per Game_y', 'xGA per Game_y', 'xG per Game Diff_y', 'xGA per Game Diff_y']]
    away_xG = away_stats['xG per Game_y'].values[0]
    away_xGA = away_stats['xGA per Game_y'].values[0]
    away_xG_diff = away_stats['xG per Game Diff_y'].values[0]
    away_xGA_diff = away_stats['xGA per Game Diff_y'].values[0]

    # calculating lambda for home team scoring at home and away team conceding away
    lambda_home = (home_xG + home_xG_diff) * (away_xGA - away_xGA_diff)
    lambda_away = (away_xG + away_xG_diff) * (home_xGA - home_xGA_diff)

    # calculate the probability of the home team scoring x goals at home and the away team conceding x goals away
    home_probs = poisson.pmf(range(8), lambda_home)
    away_probs = poisson.pmf(range(8), lambda_away)

    # calculate probabilities of different scorlines
    score_prob = np.outer(home_probs, away_probs)
    max_score = score_prob.shape[0] - 1

    # calculating probabilities of each outcome (win, draw, lose)
    home_win_prob = np.sum(np.tril(score_prob, -1))
    draw_prob = np.sum(np.diag(score_prob))
    away_win_prob = np.sum(np.triu(score_prob, 1))
    
    # calculate the total sum of probabilities
    total_prob = home_win_prob + draw_prob + away_win_prob

    # normalize probabilities
    home_win_prob /= total_prob
    draw_prob /= total_prob
    away_win_prob /= total_prob


    # return results as a dictionary
    return {
        'home_team': home_team,
        'away_team': away_team,
        'home_win_prob': home_win_prob,
        'draw_prob': draw_prob,
        'away_win_prob': away_win_prob
    }

# Function for predicting Over/Under - Goals
def fbref_predict_game_result_Goals(home_team, away_team, df_merged):
    from scipy.stats import poisson
    
    # getting HOME team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['H_GF per Game_x', 'H_GA per Game_x']]
    home_G = home_stats['H_GF per Game_x'].values[0]
    home_GA = home_stats['H_GA per Game_x'].values[0]
    
    # getting AWAY team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['A_GF per Game_y', 'A_GA per Game_y']]
    away_G = away_stats['A_GF per Game_y'].values[0]
    away_GA = away_stats['A_GA per Game_y'].values[0]
    
    # calculating lambda for each team
    lambda_home = home_G * away_GA
    lambda_away = away_G * home_GA
    
    # calculating the probability of the home team scoring x goals at home and the away team conceding x goals away
    prob = 0
    for i in range(3):
        for j in range(3):
            prob += poisson.pmf(i, lambda_home) * poisson.pmf(j, lambda_away)
            
    # calculating the probability of the game having over 2 goals
    prob_over_2 = 1 - poisson.cdf(2, lambda_home) * poisson.cdf(2, lambda_away)
    prob_over_1 = 1 - poisson.cdf(1, lambda_home) * poisson.cdf(1, lambda_away)
    prob_over_3 = 1 - poisson.cdf(3, lambda_home) * poisson.cdf(3, lambda_away)
    
    # calculating probability of each team having +1.5 goals
    prob_over_2_home = 1 - poisson.cdf(2, lambda_home)
    prob_over_2_away = 1 - poisson.cdf(2, lambda_away)
    

    #calculate total xG
    lambda_total = lambda_home + lambda_away
    

    return {"home_team": home_team, "away_team": away_team, "lambda_home": lambda_home, "lambda_away": lambda_away, 
            "prob_over_2_goals": prob_over_2, "prob_over_1_goal": prob_over_1, "prob_over_3_goals": prob_over_3, 
            "h_+1.5": prob_over_2_home, "a_+1.5": prob_over_2_away,  "expected_goals": lambda_total}

# Function for predicting H2H - Goals
def fbref_predict_game_winner_Goals(home_team, away_team, df_merged):
    from scipy.stats import poisson
    
    # getting HOME team stats
    home_stats = df_merged.loc[df_merged['Team'] == home_team, ['H_GF per Game_x', 'H_GA per Game_x']]
    home_G = home_stats['H_GF per Game_x'].values[0]
    home_GA = home_stats['H_GA per Game_x'].values[0]
    
    # getting AWAY team stats
    away_stats = df_merged.loc[df_merged['Team'] == away_team, ['A_GF per Game_y', 'A_GA per Game_y']]
    away_G = away_stats['A_GF per Game_y'].values[0]
    away_GA = away_stats['A_GA per Game_y'].values[0]
    
    # calculating lambda for each team
    lambda_home = home_G * away_GA
    lambda_away = away_G * home_GA
    
    # calculating probability of the home team scoring x goals at home and the away team conceding x goals away
    home_probs = poisson.pmf(range(8), lambda_home)
    away_probs = poisson.pmf(range(8), lambda_away)
    
    # calculating probabilities of different scorelines
    score_prob = np.outer(home_probs, away_probs)
    max_score = score_prob.shape[0] - 1
    
    # calculating probabilities of each outcome (win, draw, lose)
    home_win_prob = np.sum(np.tril(score_prob, -1))
    draw_prob = np.sum(np.diag(score_prob))
    away_win_prob = np.sum(np.triu(score_prob, 1))
    
    # calculate the total sum of probabilities
    total_prob = home_win_prob + draw_prob + away_win_prob

    # normalize probabilities
    home_win_prob /= total_prob
    draw_prob /= total_prob
    away_win_prob /= total_prob


    # return results as a dictionary
    return {
        'home_team': home_team,
        'away_team': away_team,
        'home_win_prob': home_win_prob,
        'draw_prob': draw_prob,
        'away_win_prob': away_win_prob
    }



