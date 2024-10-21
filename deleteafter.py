

import json

# Load the JSON file into a dictionary
filename = 'fixtures_url.json'

with open(filename, 'r') as json_file:
    fixtures_url = json.load(json_file)

fixtures_url = {k: fixtures_url[k] for k in list(fixtures_url.keys())[:5]}

# Now fixtures_url contains the dictionary loaded from the JSON file
print(fixtures_url)
'''

import pandas as pd

df_ou = pd.read_csv('Predictions/H2H_Predictions_Official.csv') # - uncomment when ready
df_h2h = pd.read_csv('Predictions/OU_Predictions_Official.csv')


# change columns to lowercase to avoid problems
df_ou.columns = df_ou.columns.str.lower()
df_h2h.columns = df_h2h.columns.str.lower()

# final tweaks for column names in H2H dataframe
# df_h2h.rename(columns={'home (%)': 'over_1_5', '+2.5(%)':'over_2_5', '+3.5(%)': 'over_3_5', 'h+1.5(%)':'home_over_1_5', 'a+1.5(%)':'away_over_1_5', 'r+1.5': 'r_1_5', 'r+2.5':'r_2_5', 'rh+1.5':'r_home_1_5', 'ra+1.5':'r_away_1_5'}, inplace=True) ## Add the H2H later

df_ou= pd.merge(df_h2h, df_ou, on=['league','home_team', 'away_team', 'source'])

print(df_ou.head())

'''