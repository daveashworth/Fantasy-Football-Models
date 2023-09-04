import pandas as pd
import numpy as np
import re
import nfl_data_py as nfl
import redis
import pickle
import matplotlib.pyplot as plt

r = redis.Redis(host='localhost', port=6379)

number_of_teams = 10
is_flex = True
ppr = 1
rush_yards = 0.1
receiving_yards = 0.1
rush_td = 6
receiving_td = 6
pass_yards = 0.04
pass_td = 4
fmb = -2
interception = -2

# projections
# espn_projections = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2019%20ESPN%20fantasy%20projections%20new.csv")
# espn_adp = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/adp/2019%20espn%20adp.csv")
# nfl_elo = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/team%20elo%20data/nfl_elo.csv")
# projections_2010 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2010%20fantasy%20projections.csv")
# projections_2012 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2012%20fantasy%20projections.csv")
# projections_2013 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2013%20fantasy%20projections.csv")
# projections_2014 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2014%20fantasy%20projections.csv")
# projections_2015 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2015%20fantasy%20projections.1.csv")
# projections_2016 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2016%20fantasy%20projections.csv")
# projections_2017 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2017%20projections%20new.csv")
# projections_2018 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2018%20projections%20new%20new.csv")
# projections_2019 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2019%20ppr%20ranks%20-%202019%20ppr%20ranks.csv.csv")
# projections_2020 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2020%20projections%20updated%20-%20ff%202020%20projections%20positional.csv")
# projections_2021 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2021%20projections%20-%202021%20projections%20pdf.csv.csv")
# projections_2022 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2022%20nfl%20projections%20-%20nfl%20ppr%202022%20rankings.csv.csv")
# projections_2023 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/projections/2023%20projections%20-%20NFL23_CS_PPR300.csv.csv")

# # depth charts
# depth_chart_2023 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2023%20depth%20charts%20-%20NFL23_CS_Depth.csv.csv")
# depth_chart_2022 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2022%20depth%20charts%20-%202022%20depth%20charts.csv.csv")
# depth_chart_2021 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2021%20depth%20charts%20-%202021%20depth%20charts.csv.csv")
# depth_chart_2020 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2020%20depth%20charts%20-%20NFLDK2020_CS_PPR_DepthChart.csv")
# depth_chart_2019 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2019%20espn%20depth%20charts.csv")
# depth_chart_2018 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2018%20depth%20charts.1.csv")
# depth_chart_2017 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2017%20depth%20charts.1.csv")
# depth_chart_2016 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2016%20depth%20charts.csv")
# depth_chart_2015 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2015%20depth%20charts.csv")
# depth_chart_2014 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2014%20depth%20charts.csv")
# depth_chart_2012 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2012%20depth%20charts.csv")
# depth_chart_2010 = pd.read_csv("https://raw.githubusercontent.com/jsoslow2/Fantasy-Football-Models/master/data/depth%20charts/2010%20depth%20charts.1.csv")


# # Splitting the 'Name' column to extract names, teams, and positions
# espn_adp['split_name'] = espn_adp['Name'].str.split('\n')
# espn_adp['Player_Name'] = espn_adp['split_name'].str[0]
# espn_adp['Team_Position'] = espn_adp['split_name'].str[1]
# espn_adp['Team'] = espn_adp['Team_Position'].str[:-2]
# espn_adp['position'] = espn_adp['Team_Position'].str[-2:]

# # Cleaning team names for 'ST' position
# espn_adp.loc[espn_adp['position'] == 'ST', 'Team'] = espn_adp['Team_Position'].str[:-3]

# # Initialize injury columns
# espn_adp['isQuestionable'] = 0
# espn_adp['isSuspended'] = 0
# espn_adp['isOut'] = 0
# espn_adp['isDoubtful'] = 0

# # Identify and clean injury markers from names
# injury_mappings = {
#     'Q': ('isQuestionable', 1, -2),
#     'SSPD': ('isSuspended', 1, -4),
#     'O': ('isOut', 1, -2),
#     'D': ('isDoubtful', 1, -2)
# }

# for marker, (column, value, slice_index) in injury_mappings.items():
#     mask = espn_adp['Player_Name'].str.endswith(marker)
#     espn_adp.loc[mask, 'Player_Name'] = espn_adp['Player_Name'].str[:slice_index]
#     espn_adp.loc[mask, column] = value

# # Drop temporary columns used for splitting
# espn_adp.drop(columns=['split_name', 'Team_Position', 'Name'], inplace=True)

# espn_adp.rename(columns={'Player_Name': 'Name'}, inplace=True)

# desired_columns_order = ['Rank', 'Name', 'ADP', 'Change', 'Team', 'position','isQuestionable', 'isOut', 'isSuspended', 'isDoubtful']
# espn_adp = espn_adp[desired_columns_order]



# ### Combining all past projections tables into one DataFrame ###
# ### 2010 ###
# projections_2010['Name'] = ""
# projections_2010['team'] = ""

# # Splitting and extracting information from 'rank' and 'name,team' columns
# projections_2010['split_rank'] = projections_2010['rank'].str.split(" ")
# projections_2010['split_name_team'] = projections_2010['name,team'].str.split(" ")
# projections_2010['split_name_comma'] = projections_2010['name,team'].str.split(",")

# # Iterating through rows to populate 'Name' and 'team' columns
# for idx, row in projections_2010.iterrows():
#     rank = row['split_rank'][1][1:-1]

#     items = len(row['split_name_team'])
#     team = row['split_name_team'][items - 1]
#     name = row['split_name_comma'][0]

#     projections_2010.at[idx, 'rank'] = rank
#     projections_2010.at[idx, 'team'] = team
#     projections_2010.at[idx, 'Name'] = name

#     # Modifying the 'Name' for 'ST' position
#     if row['position'] == "ST":
#         projections_2010.at[idx, 'Name'] = team + " D/ST"

# # Hardcoded corrections for 'ST' position teams
# st_positions = projections_2010[projections_2010['position'] == "ST"].index
# team_corrections = ["NYJ", "PHI", "GB", "BAL", "DAL", "SF", "MIN", "PIT", "CIN",
#                     "NO", "DEN", "NYG", "NE", "CLE", "HOU"]
# for index, team in zip(st_positions, team_corrections):
#     projections_2010.at[index, 'team'] = team

# # Additional transformations
# projections_2010['rank'] = projections_2010['rank'].astype(int)
# projections_2010['year'] = 2010
# projections_2010 = projections_2010.sort_values(by='rank')

# desired_columns_order_2010 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2010 = projections_2010[desired_columns_order_2010]



# ### 2012 ###
# projections_2012['Name'] = ""
# projections_2012['team'] = ""
# projections_2012['position'] = ""

# # Splitting and extracting information from 'name, team' and 'rank, position' columns
# projections_2012['split_name_team'] = projections_2012['name, team'].str.split(",")
# projections_2012['split_rank_position'] = projections_2012['rank, position'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2012.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_rank_position'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2012.at[idx, 'Name'] = name
#     projections_2012.at[idx, 'team'] = team
#     projections_2012.at[idx, 'position'] = position

# # Additional transformations
# projections_2012['rank'] = range(1, len(projections_2012) + 1)
# projections_2012['year'] = 2012

# # Reordering columns as specified
# desired_columns_order_2012 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2012 = projections_2012[desired_columns_order_2012]



# ### 2013 ###
# projections_2013['Name'] = ""
# projections_2013['team'] = ""
# projections_2013['position'] = ""

# # Splitting and extracting information from 'name,team' and 'rank,position' columns
# projections_2013['split_name_team'] = projections_2013['name,team'].str.split(",")
# projections_2013['split_rank_position'] = projections_2013['rank,position'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2013.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_rank_position'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2013.at[idx, 'Name'] = name
#     projections_2013.at[idx, 'team'] = team
#     projections_2013.at[idx, 'position'] = position

# # Additional transformations
# projections_2013['rank'] = range(1, len(projections_2013) + 1)
# projections_2013['year'] = 2013

# # Reordering columns as specified
# desired_columns_order_2013 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2013 = projections_2013[desired_columns_order_2013]



# ### 2014 ###
# projections_2014['Name'] = ""
# projections_2014['team'] = ""
# projections_2014['position'] = ""

# # Splitting and extracting information from 'Name, Team' and 'Rank' columns
# projections_2014['split_name_team'] = projections_2014['Name, Team'].str.split(",")
# projections_2014['split_rank'] = projections_2014['Rank'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2014.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_rank'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2014.at[idx, 'Name'] = name
#     projections_2014.at[idx, 'team'] = team
#     projections_2014.at[idx, 'position'] = position

# # Additional transformations
# projections_2014['rank'] = range(1, len(projections_2014) + 1)
# projections_2014['year'] = 2014

# # Reordering columns as specified
# desired_columns_order_2014 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2014 = projections_2014[desired_columns_order_2014]



# ### 2015 ###
# projections_2015['Name'] = ""
# projections_2015['team'] = ""
# projections_2015['position'] = ""

# projections_2015['split_name_team'] = projections_2015['name team'].str.split(",")
# projections_2015['split_rank'] = projections_2015['position rank'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2015.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_rank'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2015.at[idx, 'Name'] = name
#     projections_2015.at[idx, 'team'] = team
#     projections_2015.at[idx, 'position'] = position

# # Additional transformations
# projections_2015['rank'] = range(1, len(projections_2015) + 1)
# projections_2015['year'] = 2015

# # Reordering columns as specified
# desired_columns_order_2015 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2015 = projections_2015[desired_columns_order_2015]



# ### 2016 ###
# projections_2016['Name'] = projections_2016['RANK, PLAYER'].str.replace("^.*?\\.", "", regex=True)

# # Reordering columns to place 'Name' first
# cols = ['Name'] + [col for col in projections_2016 if col != 'Name']
# projections_2016 = projections_2016[cols]

# # Adding a 'rank' column with a sequence of integers and 'year' column with the value 2016
# projections_2016['rank'] = range(1, len(projections_2016) + 1)
# projections_2016['year'] = 2016

# projections_2016.rename(columns={'POS': 'position', 'TEAM': 'team'}, inplace=True)

# # Reordering columns as specified
# desired_columns_order_2016 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2016 = projections_2016[desired_columns_order_2016]



# ### 2017 ###
# projections_2017['Name'] = ""
# projections_2017['team'] = ""
# projections_2017['position'] = ""

# # Splitting and extracting information from 'NameTeam' and 'positionRank' columns
# projections_2017['split_name_team'] = projections_2017['NameTeam'].str.split(",")
# projections_2017['split_position_rank'] = projections_2017['positionRank'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2017.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_position_rank'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2017.at[idx, 'Name'] = name
#     projections_2017.at[idx, 'team'] = team
#     projections_2017.at[idx, 'position'] = position

# # Additional transformations
# projections_2017['rank'] = range(1, len(projections_2017) + 1)
# projections_2017['year'] = 2017

# # Reordering columns as specified
# desired_columns_order_2017 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2017 = projections_2017[desired_columns_order_2017]



# ### 2018 ###
# projections_2018['Name'] = projections_2018['Player'].str.replace("^.*?\\.", "", regex=True)

# # Reordering columns to place 'Name' first
# cols_2018 = ['Name'] + [col for col in projections_2018 if col != 'Name']
# projections_2018 = projections_2018[cols_2018]

# # Adding a 'rank' column with a sequence of integers and 'year' column with the value 2018
# projections_2018['rank'] = range(1, len(projections_2018) + 1)
# projections_2018['year'] = 2018

# projections_2018.rename(columns={'POS': 'position', 'TEAM': 'team'}, inplace=True)

# # Reordering columns as specified
# desired_columns_order_2018 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2018 = projections_2018[desired_columns_order_2018]



# ### 2019 ###
# projections_2019['Name'] = ""
# projections_2019['team'] = ""
# projections_2019['position'] = ""

# # Splitting and extracting information from 'name team' and 'position rank' columns
# projections_2019['split_name_team'] = projections_2019['name team'].str.split(",")
# projections_2019['split_position_rank'] = projections_2019['position rank'].str.split(" ")

# # Iterating through rows to populate 'Name', 'team', and 'position' columns
# for idx, row in projections_2019.iterrows():
#     name = row['split_name_team'][0]
#     team = row['split_name_team'][1].strip()

#     pos_rank = row['split_position_rank'][1][1:-1]
#     position = ''.join([i for i in pos_rank if not i.isdigit()])
#     if position == "DEF":
#         position = "ST"

#     projections_2019.at[idx, 'Name'] = name
#     projections_2019.at[idx, 'team'] = team
#     projections_2019.at[idx, 'position'] = position

# # Additional transformations
# projections_2019['rank'] = range(1, len(projections_2019) + 1)
# projections_2019['year'] = 2019

# # Reordering columns as specified
# desired_columns_order_2019 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2019 = projections_2019[desired_columns_order_2019]



# ### 2020 ###
# projections_2020['Name'] = ""
# projections_2020['team'] = ""

# # Splitting 'NameTeam' column
# projections_2020['split_name_team'] = projections_2020['NameTeam'].str.split(",")

# # Iterating through rows to populate 'Name' and 'team' columns
# for idx, row in projections_2020.iterrows():
#     name = row['split_name_team'][0].strip()
#     team = row['split_name_team'][1].strip()

#     # Extract the numeric part of the 'Rank' column
#     rank = re.search('\((\d+)\)', row['Rank']).group(1)

#     projections_2020.at[idx, 'Name'] = name
#     projections_2020.at[idx, 'team'] = team
#     projections_2020.at[idx, 'rank'] = rank

# # Convert 'Rank' column to numeric and add 'year' column with the value 2020
# projections_2020['rank'] = projections_2020['rank'].astype(int)
# projections_2020['year'] = 2020

# projections_2020.rename(columns={'Position': 'position'}, inplace=True)

# # Reordering columns as specified
# desired_columns_order_2020 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2020 = projections_2020[desired_columns_order_2020]



# ### 2021 ###
# projections_2021['Name'] = ''
# projections_2021['team'] = ''
# projections_2021['position'] = ''

# for idx, row in projections_2021.iterrows():
#     # Split name and team
#     name, team = row['name team'].split(', ')
#     projections_2021.at[idx, 'Name'] = name
#     projections_2021.at[idx, 'team'] = team

#     # Extract position and rank
#     pos_rank = re.search('\((\D+\d+)\)', row['position rank']).group(1)
#     position = re.search('\D+', pos_rank).group(0)
#     if position == "DEF":
#         position = "ST"
#     projections_2021.at[idx, 'position'] = position

# # Add rank and year columns
# projections_2021['rank'] = projections_2021.index + 1
# projections_2021['year'] = 2021

# # Reordering columns as specified
# desired_columns_order_2021 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2021 = projections_2021[desired_columns_order_2021]



# ### 2022 ###
# projections_2022['Name'] = ''
# projections_2022['team'] = ''
# projections_2022['position'] = ''

# for idx, row in projections_2022.iterrows():
#     # Split name and team
#     name, team = row['name team'].split(', ')
#     projections_2022.at[idx, 'Name'] = name
#     projections_2022.at[idx, 'team'] = team

#     # Extract position and rank
#     pos_rank = re.search('\((\D+\d+)\)', row['position rank']).group(1)
#     position = re.search('\D+', pos_rank).group(0)
#     if position == "DEF":
#         position = "ST"
#     projections_2022.at[idx, 'position'] = position

# # Add rank and year columns
# projections_2022['rank'] = projections_2022.index + 1
# projections_2022['year'] = 2022

# # Reordering columns as specified
# desired_columns_order_2022 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2022 = projections_2022[desired_columns_order_2022]




# ### 2023 ###
# projections_2023['Name'] = ''
# projections_2023['team'] = ''
# projections_2023['position'] = ''

# for idx, row in projections_2023.iterrows():
#     # Split name and team
#     name, team = row['name team'].split(', ')
#     projections_2023.at[idx, 'Name'] = name
#     projections_2023.at[idx, 'team'] = team

#     # Extract position and rank
#     pos_rank = re.search('\((\D+\d+)\)', row['position rank']).group(1)
#     position = re.search('\D+', pos_rank).group(0)
#     if position == "DEF":
#         position = "ST"
#     projections_2023.at[idx, 'position'] = position

# # Add rank and year columns
# projections_2023['rank'] = projections_2023.index + 1
# projections_2023['year'] = 2023

# # Reordering columns as specified
# desired_columns_order_2023 = ['rank', 'position', 'Name', 'team', 'year']
# projections_2023 = projections_2023[desired_columns_order_2023]




# all_projections = pd.concat([projections_2010, projections_2012, projections_2013, projections_2014, projections_2015,
#                               projections_2016, projections_2017, projections_2018, projections_2019, projections_2020,
#                               projections_2021, projections_2022, projections_2023], ignore_index=True)


# filtered = all_projections[all_projections['year'] == 2023]
# print(filtered.head(10))



### Now we're cleaning the depth charts ###

### 2023 ###
# depth_chart_2023['Name'] = depth_chart_2023['name rank'].str.split(' \(').str[0]

# depth_chart_2023['year'] = 2023

# desired_depth_chart_colums = ['Name', 'year', 'depthChart']
# depth_chart_2023 = depth_chart_2023[desired_depth_chart_colums]


# ### 2022 ###
# depth_chart_2022['Name'] = depth_chart_2022['name rank'].str.split(' \(').str[0]

# depth_chart_2022['year'] = 2022
# depth_chart_2022 = depth_chart_2022[desired_depth_chart_colums]


# ### 2021 ###
# depth_chart_2021['Name'] = depth_chart_2021['name rank'].str.split(' \(').str[0]

# depth_chart_2021['year'] = 2021
# depth_chart_2021 = depth_chart_2021[desired_depth_chart_colums]


# ### 2020 ###
# depth_chart_2020['Name'] = depth_chart_2020['Name rank'].str.split(' \(').str[0]

# depth_chart_2020['year'] = 2020
# depth_chart_2020 = depth_chart_2020[desired_depth_chart_colums]


# ### 2019 ###
# depth_chart_2019['Name'] = depth_chart_2019['Name rank'].str.split(' \(').str[0]

# depth_chart_2019['year'] = 2019
# depth_chart_2019 = depth_chart_2019[desired_depth_chart_colums]


# ### 2018 ###
# depth_chart_2018.columns = ['depthChart', 'nameRank', 'team']

# # Removing the first row
# depth_chart_2018 = depth_chart_2018.iloc[1:]

# # Extracting player names and assigning to new 'Name' column
# depth_chart_2018['Name'] = depth_chart_2018['nameRank'].str.split(' \(').str[0]

# depth_chart_2018['year'] = 2018
# depth_chart_2018 = depth_chart_2018[desired_depth_chart_colums]


# ### 2017 ###
# depth_chart_2017.columns = ['depthChart', 'Name position', 'team']

# depth_chart_2017['Name'] = depth_chart_2017['Name position'].str.split(' \(').str[0]

# depth_chart_2017['year'] = 2017
# depth_chart_2017 = depth_chart_2017[desired_depth_chart_colums]


# ### 2016 ###
# depth_chart_2016.columns = ['depthChart', 'Name position']

# depth_chart_2016['Name'] = depth_chart_2016['Name position'].str.split(' \(').str[0]

# depth_chart_2016['year'] = 2016
# depth_chart_2016 = depth_chart_2016[desired_depth_chart_colums]


# ### 2015 ###
# depth_chart_2015.columns = ['depthChart', 'Name position']

# depth_chart_2015['Name'] = depth_chart_2015['Name position'].str.split(' \(').str[0]

# depth_chart_2015['year'] = 2015
# depth_chart_2015 = depth_chart_2015[desired_depth_chart_colums]

# depth_chart_2015.loc[depth_chart_2015['Name'] == "Steve Smith", 'Name'] = "Steve Smith Sr."


# ### 2014 ###
# depth_chart_2014.columns = ['depthChart', 'Name position']

# depth_chart_2014['Name'] = depth_chart_2014['Name position'].str.split(' \(').str[0]

# depth_chart_2014['year'] = 2014
# depth_chart_2014 = depth_chart_2014[desired_depth_chart_colums]


# ### 2012 ###
# depth_chart_2012.columns = ['depthChart', 'Name']

# depth_chart_2012['Name'] = depth_chart_2012['Name'].str.replace(r"\s\(R\)$", "")

# # Assign the year 2012 to each row
# depth_chart_2012['year'] = 2012
# depth_chart_2012 = depth_chart_2012[desired_depth_chart_colums]


# ### 2010 ###
# depth_chart_2010.columns = ['depthChart', 'Name']

# depth_chart_2010['Name'] = depth_chart_2010['Name'].str.replace(r"\s\(R\)$", "")
# depth_chart_2010['Name'] = depth_chart_2010['Name'].str.rstrip('*')
# depth_chart_2010['Name'] = depth_chart_2010['Name'].str.rstrip()

# depth_chart_2010['year'] = 2010
# depth_chart_2010 = depth_chart_2010[desired_depth_chart_colums]


# all_depth_charts = pd.concat([depth_chart_2010, depth_chart_2012, depth_chart_2014, depth_chart_2015, depth_chart_2016,
#                               depth_chart_2017, depth_chart_2018, depth_chart_2019, depth_chart_2020, depth_chart_2021,
#                               depth_chart_2022, depth_chart_2023], ignore_index=True)

# filtered_depth_charts = all_depth_charts[all_depth_charts['year'] == 2023]


## load the rosters
def load_rosters(year):
    roster_columns = ['player_id','season','team','player_name','position']
    retrieved_rosters = r.get(f"rosters_{year}:v1")

    if retrieved_rosters:
        print(f'data loaded from redis for {year}')
        return pickle.loads(retrieved_rosters)
    else:
        print(f'loading data from github for {year}')
        retrieved_rosters = nfl.import_rosters([year], roster_columns)
        r.set(f"rosters_{year}:v1", pickle.dumps(retrieved_rosters))
        return retrieved_rosters

rosters = {}
for year in range(2009,2024):
    rosters[year] = load_rosters(year)

all_rosters = pd.concat(rosters.values(), ignore_index=True)
all_rosters.rename(columns={'player_id': 'gsis_id', 'player_name': 'full_name'}, inplace=True)

print("**** all rosters top 25 ****")
print(all_rosters.head(10))

## load the play by play data
pbp_columns = [
      "pass_attempt", "passer_player_id", "rusher_player_id",
      "receiver_player_id", "play_type", "game_id", "drive",
      "passer_player_name", "complete_pass", "yards_gained",
      "air_yards", "qb_hit", "interception", "touchdown",
      "yards_after_catch", "epa", "wpa", "air_epa", "air_wpa",
      "yac_epa", "yac_wpa", "rush_attempt", "rusher_player_name",
      "fumble", "receiver_player_name", "posteam", "defteam", "Season"]

def store_dataframe_in_redis(name, dataframe):
    r.delete(name)
    serialized_data = pickle.dumps(dataframe)
    r.set(name, serialized_data)

def fetch_dataframe_from_redis(name):
    serialized_data = r.get(name)

    if serialized_data:
      return pickle.loads(serialized_data)
    else:
      return None

def load_pbp(year):
    year_pbp = fetch_dataframe_from_redis(f"pbp_{year}:v1")

    if year_pbp is not None and year_pbp.empty == False:
        print(f'data loaded from redis for {year}')
        return year_pbp
    else:
      print(f'loading data from github for {year}')
      year_pbp = nfl.import_pbp_data([year])
      store_dataframe_in_redis(f'pbp_{year}:v1', year_pbp)
      return year_pbp

def find_player_name(player_names):
    if len(player_names) == 0:
        return "None"
    else:
        return player_names.value_counts().idxmax()

def calc_passing_splits(splits, pbp_df):
    pbp_df = pbp_df.copy()

    # Filter data for pass attempts and add the GameDrive column
    pbp_df = pbp_df[(pbp_df['pass_attempt'] == 1) & (pbp_df['play_type'] != 'No Play')]
    #pbp_df['GameDrive'] = pbp_df['game_id'].astype(str) + "-" + pbp_df['drive'].astype(str)
    pbp_df.loc[:, 'GameDrive'] = pbp_df['game_id'].astype(str) + "-" + pbp_df['drive'].astype(str)


    # Group by splits and compute statistics
    # This is a simple example to compute 'Attempts', more columns can be added similarly
    result = pbp_df.groupby(splits).agg(
        Player_Name = pd.NamedAgg(column='passer_player_name', aggfunc=find_player_name),
        Attempts = pd.NamedAgg(column='pass_attempt', aggfunc='count'),
        Completions = pd.NamedAgg(column='complete_pass', aggfunc='sum'),
        Drives = pd.NamedAgg(column='GameDrive', aggfunc='nunique'),
        Total_Yards = pd.NamedAgg(column='yards_gained', aggfunc='sum'),
        Total_Yards_8 = pd.NamedAgg(column='yards_gained', aggfunc=lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_Yards_4 = pd.NamedAgg(column='yards_gained', aggfunc=lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_Raw_AirYards = pd.NamedAgg(column='air_yards', aggfunc='sum'),
        Total_Comp_AirYards = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'air_yards']).sum()),
        TimesHit = pd.NamedAgg(column='qb_hit', aggfunc='sum'),
        Interceptions = pd.NamedAgg(column='interception', aggfunc='sum'),
        Interceptions_8 = pd.NamedAgg(column='interception', aggfunc=lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Interceptions_4 = pd.NamedAgg(column='interception', aggfunc=lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        TDs = pd.NamedAgg(column='touchdown', aggfunc='sum'),
        TDs_8 = pd.NamedAgg(column='touchdown', aggfunc=lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        TDs_4 = pd.NamedAgg(column='touchdown', aggfunc=lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Air_TDs = pd.NamedAgg(column='touchdown', aggfunc=lambda x: ((pbp_df.loc[x.index, 'yards_after_catch'] == 0).astype(int) * x).sum()),
        Total_EPA = pd.NamedAgg(column='epa', aggfunc='sum'),
        Success_Rate = pd.NamedAgg(column='epa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        EPA_Per_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'epa']).sum() / x.sum() if x.sum() != 0 else 0),
        EPA_Comp_Perc = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'epa']).sum() / abs(pbp_df.loc[x.index, 'epa']).sum()),
        Total_WPA = pd.NamedAgg(column='wpa', aggfunc='sum'),
        Win_Success_Rate = pd.NamedAgg(column='wpa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        WPA_per_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'wpa']).sum() / x.sum() if x.sum() != 0 else 0),
        WPA_Comp_Perc = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'wpa']).sum() / abs(pbp_df.loc[x.index, 'wpa']).sum() if abs(pbp_df.loc[x.index, 'wpa']).sum() != 0 else 0),
        Total_Clutch_EPA = pd.NamedAgg(column='epa', aggfunc=lambda x: (x * abs(pbp_df.loc[x.index, 'wpa'])).sum()),
        airEPA_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'air_epa']).sum()),
        airEPA_Incomp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (pbp_df.loc[x.index, 'air_epa'] * (x == 0)).sum()),
        Total_Raw_airEPA = pd.NamedAgg(column='air_epa', aggfunc='sum'),
        air_Success_Rate = pd.NamedAgg(column='air_epa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        air_Comp_Success_Rate = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: ((x * pbp_df.loc[x.index, 'air_epa']) > 0).sum() / len(x)),
        airWPA_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'air_wpa']).sum()),
        airWPA_Incomp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (pbp_df.loc[x.index, 'air_wpa'] * (x == 0)).sum()),
        Total_Raw_airWPA = pd.NamedAgg(column='air_wpa', aggfunc='sum'),
        air_Win_Success_Rate = pd.NamedAgg(column='air_wpa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        air_Comp_Win_Success_Rate = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: ((x * pbp_df.loc[x.index, 'air_wpa']) > 0).sum() / len(x)),
        yacEPA_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'yac_epa']).sum()),
        yacEPA_Drop = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (pbp_df.loc[x.index, 'yac_epa'] * (x == 0)).sum()),
        Total_yacEPA = pd.NamedAgg(column='yac_epa', aggfunc='sum'),
        yacWPA_Comp = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (x * pbp_df.loc[x.index, 'yac_wpa']).sum()),
        yacWPA_Drop = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: (pbp_df.loc[x.index, 'yac_wpa'] * (x == 0)).sum()),
        Total_yacWPA = pd.NamedAgg(column='yac_wpa', aggfunc='sum'),
        yac_Success_Rate = pd.NamedAgg(column='yac_epa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        yac_Rec_Success_Rate = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: ((x * pbp_df.loc[x.index, 'yac_epa']) > 0).sum() / len(x)),
        yac_Win_Success_Rate = pd.NamedAgg(column='yac_wpa', aggfunc=lambda x: (x > 0).sum() / len(x)),
        yac_Complete_Win_Success_Rate = pd.NamedAgg(column='complete_pass', aggfunc=lambda x: ((x * pbp_df.loc[x.index, 'yac_wpa']) > 0).sum() / len(x)),
    )

    # Additional calculations can be performed on the result DataFrame
    result['Comp_Perc'] = result['Completions'] / result['Attempts']
    result['Yards_Per_Att'] = result['Total_Yards'] / result['Attempts']
    result['Yards_Per_Comp'] = result['Total_Yards'] / result['Completions']
    result['Yards_Per_Drive'] = result['Total_Yards'] / result['Drives']
    result['Raw_AirYards_per_Att'] = result['Total_Raw_AirYards'] / result['Attempts']
    result['Comp_AirYards_per_Att'] = result['Total_Comp_AirYards'] / result['Attempts']
    result['Raw_AirYards_per_Comp'] = result['Total_Raw_AirYards'] / result['Completions']
    result['Comp_AirYards_per_Comp'] = result['Total_Comp_AirYards'] / result['Completions']
    result['Raw_AirYards_per_Drive'] = result['Total_Raw_AirYards'] / result['Drives']
    result['Comp_AirYards_per_Drive'] = result['Total_Comp_AirYards'] / result['Drives']
    result['PACR'] = result['Total_Yards'] / result['Total_Raw_AirYards']
    result['TimesHit_per_Drive'] = result['TimesHit'] / result['Drives']
    result['aPACR'] = (result['Total_Yards'] + (20 * result['TDs']) - (45 * result['Interceptions'])) / result['Total_Raw_AirYards']
    result['Air_TD_Rate'] = result['Air_TDs'] / result['TDs']
    result['TD_to_Int'] = result['TDs'] / result['Interceptions']
    result['EPA_per_Att'] = result['Total_EPA'] / result['Attempts']
    result['TD_per_Att'] = result['TDs'] / result['Attempts']
    result['Air_TD_per_Att'] = result['Air_TDs'] / result['Attempts']
    result['Int_per_Att'] = result['Interceptions'] / result['Attempts']
    result['TD_per_Comp'] = result['TDs'] / result['Completions']
    result['Air_TD_per_Comp'] = result['Air_TDs'] / result['Completions']
    result['TD_per_Drive'] = result['TDs'] / result['Drives']
    result['Air_TD_per_Drive'] = result['Air_TDs'] / result['Drives']
    result['Int_per_Drive'] = result['Interceptions'] / result['Drives']
    result['EPA_per_Drive'] = result['Total_EPA'] / result['Drives']
    result['WPA_per_Att'] = result['Total_WPA'] / result['Attempts']
    result['WPA_per_Drive'] = result['Total_WPA'] / result['Drives']
    result['Clutch_EPA_per_Att'] = result['Total_Clutch_EPA'] / result['Attempts']
    result['Clutch_EPA_per_Drive'] = result['Total_Clutch_EPA'] / result['Drives']
    result['Raw_airEPA_per_Att'] = result['Total_Raw_airEPA'] / result['Attempts']
    result['Raw_airEPA_per_Drive'] = result['Total_Raw_airEPA'] / result['Drives']
    result['epa_PACR'] = result['Total_EPA'] / result['Total_Raw_airEPA']
    result['airEPA_per_Att'] = result['airEPA_Comp'] / result['Attempts']
    result['airEPA_per_Comp'] = result['airEPA_Comp'] / result['Completions']
    result['airEPA_per_Drive'] = result['airEPA_Comp'] / result['Drives']
    result['wpa_PACR'] = result['Total_WPA'] / result['Total_Raw_airWPA']
    result['Raw_airWPA_per_Att'] = result['Total_Raw_airWPA'] / result['Attempts']
    result['Raw_airWPA_per_Drive'] = result['Total_Raw_airWPA'] / result['Drives']
    result['airWPA_per_Att'] = result['airWPA_Comp'] / result['Attempts']
    result['airWPA_per_Comp'] = result['airWPA_Comp'] / result['Completions']
    result['airWPA_per_Drive'] = result['airWPA_Comp'] / result['Drives']
    result['yacEPA_per_Att'] = result['Total_yacEPA'] / result['Attempts']
    result['yacEPA_per_Comp'] = result['yacEPA_Comp'] / result['Completions']
    result['yacEPA_Rec_per_Drive'] = result['yacEPA_Comp'] / result['Drives']
    result['yacEPA_Drop_per_Drive'] = result['yacEPA_Drop'] / result['Drives']

    return result.reset_index()

def calc_rushing_splits(splits, pbp_df):
    pbp_df = pbp_df.copy()

    # Filter to only rush attempts:
    pbp_df = pbp_df[(pbp_df['rush_attempt'] == 1) & (pbp_df['play_type'] != "No Play")]
    pbp_df['GameDrive'] = pbp_df['game_id'].astype(str) + "-" + pbp_df['drive'].astype(str)

    # Aggregate calculations
    result = pbp_df.groupby(splits).agg(
        Player_Name=('rusher_player_name', find_player_name),
        Carries=('rush_attempt', 'count'),
        Drives=('GameDrive', 'nunique'),
        Total_Yards=('yards_gained', 'sum'),
        Total_Yards_8=('yards_gained', lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_Yards_4=('yards_gained', lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Fumbles=('fumble', 'sum'),
        TDs=('touchdown', 'sum'),
        TDs_8=('touchdown', lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        TDs_4=('touchdown', lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_EPA=('epa', 'sum'),
        Success_Rate=('epa', lambda x: (x > 0).sum() / len(x)),
        EPA_Ratio=('epa', lambda x: (x > 0).sum() / abs(x).sum() if abs(x).sum() != 0 else 0),
        Total_WPA=('wpa', 'sum'),
        Win_Success_Rate=('wpa', lambda x: (x > 0).sum() / len(x)),
        WPA_Ratio=('wpa', lambda x: (x > 0).sum() / abs(x).sum() if abs(x).sum() != 0 else 0),
        Total_Clutch_EPA=('epa', lambda x: (x * abs(pbp_df.loc[x.index, 'wpa'])).sum()),
    )

    # Derived calculations
    result['Car_per_Drive'] = result['Carries'] / result['Drives']
    result['Yards_per_Car'] = result['Total_Yards'] / result['Carries']
    result['Yards_per_Drive'] = result['Total_Yards'] / result['Drives']
    result['TD_to_Fumbles'] = result.apply(lambda row: row['TDs'] / row['Fumbles'] if row['Fumbles'] != 0 else 0, axis=1)
    result['EPA_per_Car'] = result['Total_EPA'] / result['Carries']
    result['TD_per_Car'] = result['TDs'] / result['Carries']
    result['Fumbles_per_Car'] = result['Fumbles'] / result['Carries']
    result['Fumbles_per_Drive'] = result['Fumbles'] / result['Drives']
    result['TD_Drive'] = result['TDs'] / result['Drives']
    result['EPA_per_Drive'] = result['Total_EPA'] / result['Drives']
    result['WPA_per_Drive'] = result['Total_WPA'] / result['Drives']
    result['WPA_per_Car'] = result['Total_WPA'] / result['Carries']
    result['Clutch_EPA_per_Car'] = result['Total_Clutch_EPA'] / result['Carries']
    result['Clutch_EPA_per_Drive'] = result['Total_Clutch_EPA'] / result['Drives']

    return result.reset_index()

def calc_receiving_splits(splits, pbp_df):
    pbp_df = pbp_df.copy()

    # Filter to only pass attempts:
    pbp_df = pbp_df[(pbp_df['pass_attempt'] == 1) & (pbp_df['play_type'] != "No Play")]
    pbp_df['GameDrive'] = pbp_df['game_id'].astype(str) + "-" + pbp_df['drive'].astype(str)

    # Aggregate calculations
    result = pbp_df.groupby(splits).agg(
        Player_Name=('receiver_player_name', find_player_name),
        Targets=('pass_attempt', 'count'),
        Receptions=('complete_pass', 'sum'),
        Receptions_8=('complete_pass', lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Receptions_4=('complete_pass', lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Drives=('GameDrive', 'nunique'),
        Total_Yards=('yards_gained', 'sum'),
        Total_Yards_8=('yards_gained', lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_Yards_4=('yards_gained', lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        Total_Raw_YAC=('yards_after_catch', 'sum'),
        Total_Caught_YAC=('yards_after_catch', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        Total_Dropped_YAC=('yards_after_catch', lambda x: ((pbp_df.loc[x.index, 'complete_pass'] == 0).astype(int) * x).sum()),
        Fumbles=('fumble', 'sum'),
        TDs=('touchdown', 'sum'),
        TDs_8=('touchdown', lambda x: sum([y if g > 8 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        TDs_4=('touchdown', lambda x: sum([y if g > 12 else 0 for y, g in zip(x, pbp_df.loc[x.index, 'game_number'])])),
        AC_TDs=('touchdown', lambda x: ((pbp_df.loc[x.index, 'yards_after_catch'] > 0).astype(int) * x).sum()),
        Total_EPA=('epa', 'sum'),
        Success_Rate=('epa', lambda x: (x > 0).sum() / pbp_df.loc[x.index, 'pass_attempt'].sum()),
        EPA_per_Rec=('epa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum() / pbp_df.loc[x.index, 'complete_pass'].sum() if pbp_df.loc[x.index, 'complete_pass'].sum() != 0 else 0),
        EPA_Rec_Perc=('epa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum() / abs(x).sum()),
        Total_WPA=('wpa', 'sum'),
        Win_Success_Rate=('wpa', lambda x: (x > 0).sum() / pbp_df.loc[x.index, 'pass_attempt'].sum()),
        WPA_per_Rec=('wpa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum() / pbp_df.loc[x.index, 'complete_pass'].sum() if pbp_df.loc[x.index, 'complete_pass'].sum() != 0 else 0),
        WPA_Rec_Perc=('wpa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum() / abs(x).sum() if abs(x).sum() != 0 else 0),
        Total_Clutch_EPA=('epa', lambda x: (x * abs(pbp_df.loc[x.index, 'wpa'])).sum()),
        Total_Raw_AirYards=('air_yards', 'sum'),
        Total_Caught_AirYards=('air_yards', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        Total_Raw_airEPA=('air_epa', 'sum'),
        Total_Caught_airEPA=('air_epa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        Total_Raw_airWPA=('air_wpa', 'sum'),
        Total_Caught_airWPA=('air_wpa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        yacEPA_Rec=('yac_epa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        yacEPA_Drop=('yac_epa', lambda x: ((pbp_df.loc[x.index, 'complete_pass'] == 0).astype(int) * x).sum()),
        Total_yacEPA=('yac_epa', 'sum'),
        yacWPA_Rec=('yac_wpa', lambda x: (pbp_df.loc[x.index, 'complete_pass'] * x).sum()),
        yacWPA_Drop=('yac_wpa', lambda x: ((pbp_df.loc[x.index, 'complete_pass'] == 0).astype(int) * x).sum()),
        Total_yacWPA=('yac_wpa', 'sum'),
        yac_Success_Rate = ('yac_epa', lambda x: (x > 0).sum() / len(x)),
        yac_Rec_Success_Rate = ('complete_pass', lambda x: ((x * pbp_df.loc[x.index, 'yac_epa']) > 0).sum() / len(x)),
        air_Success_Rate = ('air_epa', lambda x: (x > 0).sum() / len(x)),
        air_Rec_Success_Rate = ('complete_pass', lambda x: ((x * pbp_df.loc[x.index, 'air_epa']) > 0).sum() / len(x)),
        yac_Win_Success_Rate = ('yac_wpa', lambda x: (x > 0).sum() / len(x)),
        yac_Rec_Win_Success_Rate = ('complete_pass', lambda x: ((x * pbp_df.loc[x.index, 'yac_wpa']) > 0).sum() / len(x)),
        air_Win_Success_Rate = ('air_wpa', lambda x: (x > 0).sum() / len(x)),
        air_Rec_Win_Success_Rate = ('complete_pass', lambda x: ((x * pbp_df.loc[x.index, 'air_wpa']) > 0).sum() / len(x)),
    )

    # Derived calculations
    result['Targets_per_Drive'] = result['Targets'] / result['Drives']
    result['Rec_per_Drive'] = result['Receptions'] / result['Drives']
    result['Yards_per_Drive'] = result['Total_Yards'] / result['Drives']
    result['Yards_per_Rec'] = result['Total_Yards'] / result['Receptions']
    result['Yards_per_Target'] = result['Total_Yards'] / result['Targets']
    result['YAC_per_Target'] = result['Total_Raw_YAC'] / result['Targets']
    result['Caught_YAC_per_Target'] = result['Total_Caught_YAC'] / result['Targets']
    result['Dropped_YAC_per_Target'] = result['Total_Dropped_YAC'] / result['Targets']
    result['YAC_per_Rec'] = result['Total_Raw_YAC'] / result['Receptions']
    result['Caught_YAC_per_Rec'] = result['Total_Caught_YAC'] / result['Receptions']
    result['Dropped_YAC_per_Rec'] = result['Total_Dropped_YAC'] / result['Receptions']
    result['YAC_per_Drive'] = result['Total_Raw_YAC'] / result['Drives']
    result['Caught_YAC_per_Drive'] = result['Total_Caught_YAC'] / result['Drives']
    result['Dropped_YAC_per_Drive'] = result['Total_Dropped_YAC'] / result['Drives']
    result['Rec_Percentage'] = result['Receptions'] / result['Targets']
    result['TDs_per_Drive'] = result['TDs'] / result['Drives']
    result['Fumbles_per_Drive'] = result['Fumbles'] / result['Drives']
    result['AC_TDs_per_Drive'] = result['AC_TDs'] / result['Drives']
    result['AC_TD_Rate'] = result['AC_TDs'] / result['TDs']
    result['TD_to_Fumbles'] = result['TDs'] / result['Fumbles']
    result['EPA_per_Drives'] = result['Total_EPA'] / result['Drives']
    result['EPA_per_Target'] = result['Total_EPA'] / result['Targets']
    result['TD_per_Targets'] = result['TDs'] / result['Targets']
    result['Fumbles_per_Receptions'] = result['Fumbles'] / result['Receptions']
    result['TD_per_Rec'] = result['TDs'] / result['Receptions']
    result['WPA_per_Drive'] = result['Total_WPA'] / result['Drives']
    result['WPA_per_Target'] = result['Total_WPA'] / result['Targets']
    result['Clutch_EPA_per_Drive'] = result['Total_Clutch_EPA'] / result['Drives']
    result['PACR'] = result['Total_Yards'] / result['Total_Raw_AirYards']
    result['Raw_AirYards_per_Target'] = result['Total_Raw_AirYards'] / result['Targets']
    result['RACR'] = result['Total_Yards'] / result['Total_Raw_AirYards']
    result['Raw_airEPA_per_Drive'] = result['Total_Raw_airEPA'] / result['Drives']
    result['Caught_airEPA_per_Drive'] = result['Total_Caught_airEPA'] / result['Drives']
    result['airEPA_per_Target'] = result['Total_Raw_airEPA'] / result['Targets']
    result['Caught_airEPA_per_Target'] = result['Total_Caught_airEPA'] / result['Targets']
    result['epa_RACR'] = result['Total_EPA'] / result['Total_Raw_airEPA']
    result['Raw_airWPA_per_Drive'] = result['Total_Raw_airWPA'] / result['Drives']
    result['Caught_airWPA_per_Drive'] = result['Total_Caught_airWPA'] / result['Drives']
    result['airWPA_per_Target'] = result['Total_Raw_airWPA'] / result['Targets']
    result['Caught_airWPA_per_Target'] = result['Total_Caught_airWPA'] / result['Targets']
    result['yacEPA_per_Target'] = result['Total_yacEPA'] / result['Targets']
    result['yacEPA_per_Rec'] = result['yacEPA_Rec'] / result['Receptions']
    result['yacEPA_Rec_per_Drive'] = result['yacEPA_Rec'] / result['Drives']
    result['yacEPA_Drop_per_Drive'] = result['yacEPA_Drop'] / result['Drives']
    result['yacWPA_per_Target'] = result['Total_yacWPA'] / result['Targets']
    result['yacWPA_per_Rec'] = result['yacWPA_Rec'] / result['Receptions']
    result['yacWPA_Rec_per_Drive'] = result['yacWPA_Rec'] / result['Drives']
    result['yacWPA_Drop_per_Drive'] = result['yacWPA_Drop'] / result['Drives']
    result['wpa_RACR'] = result['Total_WPA'] / result['Total_Raw_airWPA']

    return result.reset_index()


pbps = {}
for year in range(2009, 2023):
    selected_data = load_pbp(year)
    # Adding the 'Season' column
    selected_data['Season'] = year
    pbps[year] = selected_data

### bind seasons together to make one dataset
pbp_data = pd.concat(pbps.values(), ignore_index=True)

pbp_data = pbp_data[pbp_columns]

# Arrange by Season and group by Season
pbp_data = pbp_data.sort_values(by='Season')
pbp_data['game'] = pbp_data.groupby('Season')['game_id'].transform(lambda x: x.nunique()).cumsum()

# Arrange by Season and game_id, and group by Season, game_id, and posteam
pbp_data = pbp_data.sort_values(by=['Season', 'game_id'])
pbp_data['var_temp'] = pbp_data.groupby(['Season', 'game_id', 'posteam']).cumcount() + 1
pbp_data['var_temp'] = pbp_data['var_temp'].apply(lambda x: 1 if x == 1 else 0)

# Group by Season and posteam
pbp_data['game_number'] = pbp_data.groupby(['Season', 'posteam'])['var_temp'].cumsum()
pbp_data['max'] = pbp_data.groupby(['Season', 'posteam'])['game_number'].transform('max')

pbp_data['posteam'] = pbp_data['posteam'].apply(lambda x: 'JAC' if x == 'JAX' else x)
pbp_data['defteam'] = pbp_data['defteam'].apply(lambda x: 'JAC' if x == 'JAX' else x)

# First generate stats at the Season level for each player,
# removing the observations with missing player names:
season_passing_df = calc_passing_splits(["Season", "passer_player_id"], pbp_data)
season_passing_df['passer_gsis'] = season_passing_df['passer_player_id']

# Filter out rows where 'passer_player_id' is 'None'
season_passing_df = season_passing_df[season_passing_df['passer_player_id'] != 'None']

# Sort the dataframe by 'Season' and 'Attempts' in descending order
season_passing_df = season_passing_df.sort_values(by=['Season', 'Attempts'], ascending=[True, False])

#print(season_passing_df[["Season", "Player_Name", "Comp_Perc", "Total_EPA", "Completions", "Air_TDs", "Air_TD_Rate", "EPA_Comp_Perc"]].head(10))
print("season_passing_df data check:")
print(season_passing_df.head(10))


### Now do the rushing stats
season_rushing_df = calc_rushing_splits(["Season", "rusher_player_id"], pbp_data)

# Filter out rows where 'rusher_player_id' is 'None'
season_rushing_df = season_rushing_df[season_rushing_df['rusher_player_id'] != 'None']

# Sort the dataframe by Season and Carries in descending order
season_rushing_df = season_rushing_df.sort_values(by=['Season', 'Carries'], ascending=[True, False])

print("season_rushing_df data check:")
print(season_rushing_df.head(10))



### Now do the receiving stats
season_receiving_df = calc_receiving_splits(["Season", "receiver_player_id"], pbp_data)
season_receiving_df['receiver_gsis'] = season_receiving_df['receiver_player_id']

# Filter out rows where receiver_player_id is 'None'
season_receiving_df = season_receiving_df[season_receiving_df['receiver_player_id'] != 'None']

# Sort the dataframe by 'Season' and 'Targets' in descending order
season_receiving_df = season_receiving_df.sort_values(by=['Season', 'Targets'], ascending=[True, False])

print("season_receiving_df data check:")
print(season_receiving_df.head(10))









### Merge all the dataframes into one ###
all_data_df = season_rushing_df.merge(season_receiving_df, left_on=['rusher_player_id', 'Season'], right_on=['receiver_player_id', 'Season'], how='outer')
all_data_df = all_data_df.merge(season_passing_df, left_on=['rusher_player_id', 'Season'], right_on=['passer_player_id', 'Season'], how='outer')
all_data_df['gsis_id'] = all_data_df['rusher_player_id'].combine_first(all_data_df['receiver_gsis']).combine_first(all_data_df['passer_gsis'])
all_data_df['Name'] = all_data_df['Player_Name'].combine_first(all_data_df['Player_Name_x']).combine_first(all_data_df['Player_Name_y'])


### create fantasy points
# 1. Replace infinite values with NA
all_data_df.replace([np.inf, -np.inf], np.nan, inplace=True)

# 2. Replace NA values in numeric columns with 0
numerics = all_data_df.select_dtypes(include=[np.number]).columns
all_data_df[numerics] = all_data_df[numerics].fillna(0)

# 3. Create new columns for fantasy points calculations
fantasy_points = (all_data_df['Total_Yards_x'] * rush_yards +
                                all_data_df['TDs_x'] * rush_td +
                                (all_data_df['Fumbles_x'] + all_data_df['Fumbles_y']) * fmb +
                                all_data_df['Total_Yards_y'] * receiving_yards +
                                all_data_df['TDs_y'] * receiving_td +
                                all_data_df['Receptions'] * ppr +
                                all_data_df['Total_Yards'] * pass_yards +
                                all_data_df['TDs'] * pass_td +
                                all_data_df['Interceptions'] * interception)

fantasy_points_8 = (all_data_df['Total_Yards_8_x'] * rush_yards +
                                   all_data_df['TDs_8_x'] * rush_td +
                                   all_data_df['Total_Yards_8_y'] * receiving_yards +
                                   all_data_df['TDs_8_y'] * receiving_td +
                                   all_data_df['Receptions_8'] * ppr +
                                   all_data_df['Total_Yards_8'] * pass_yards +
                                   all_data_df['TDs_8'] * pass_td
                                   )

fantasy_points_4 = (all_data_df['Total_Yards_4_x'] * rush_yards +
                                   all_data_df['TDs_4_x'] * rush_td +
                                   all_data_df['Total_Yards_4_y'] * receiving_yards +
                                   all_data_df['TDs_4_y'] * receiving_td +
                                   all_data_df['Receptions_4'] * ppr +
                                   all_data_df['Total_Yards_4'] * pass_yards +
                                   all_data_df['TDs_4'] * pass_td
                                   )

all_data_df["fantasyPoints"] = fantasy_points
all_data_df["fantasy_points_8"] = fantasy_points_8
all_data_df["fantasy_points_4"] = fantasy_points_4

# get a defragmented version of the dataframe
all_data_df = all_data_df.copy()

# 5. Remove duplicate rows from the roster dataframe
all_rosters = all_rosters.drop_duplicates()

# Rename the full_name column
all_rosters.rename(columns={'full_name': 'full_player_name'}, inplace=True)

# 6. Merge all_data_df with the all_rosters dataframe based on gsis_id and Season
all_data_df = pd.merge(all_data_df, all_rosters[['gsis_id', 'season', 'team', 'full_player_name', 'position']],
                   left_on=['gsis_id', 'Season'],
                   right_on=['gsis_id', 'season'],
                   how='left')


print("all_data_df top 25 after adding Fantasy Points and merging in rosters!")
print(all_data_df.head(10))

# 7. Filter rows where full_player_name is not NA
all_data_df = all_data_df[all_data_df['full_player_name'].notna()]

print("all_data_df top 25 after filtering out NA full_player_name!")
print(all_data_df.head(10))



### Now for the weekly data
splits_passing = ["Season", "passer_player_id", "game_number"]
season_weekly_passing_df = calc_passing_splits(splits_passing, pbp_data)
season_weekly_passing_df['passer_gsis'] = season_weekly_passing_df['passer_player_id']
season_weekly_passing_df = season_weekly_passing_df[season_weekly_passing_df['passer_player_id'] != "None"]
season_weekly_passing_df = season_weekly_passing_df.sort_values(by=["Season", "Attempts"], ascending=[True, False])

splits_receiving = ["Season", "receiver_player_id", "game_number"]
season_weekly_receiving_df = calc_receiving_splits(splits_receiving, pbp_data)
season_weekly_receiving_df = season_weekly_receiving_df[season_weekly_receiving_df['receiver_player_id'] != "None"]
season_weekly_receiving_df['receiver_gsis'] = season_weekly_receiving_df['receiver_player_id']
season_weekly_receiving_df = season_weekly_receiving_df.sort_values(by=["Season", "Targets"], ascending=[True, False])

splits_rushing = ["Season", "rusher_player_id", "game_number"]
season_weekly_rushing_df = calc_rushing_splits(splits_rushing, pbp_data)
season_weekly_rushing_df = season_weekly_rushing_df[season_weekly_rushing_df['rusher_player_id'] != "None"]
season_weekly_rushing_df = season_weekly_rushing_df.sort_values(by=["Season", "Carries"], ascending=[True, False])

# Merging rushing and receiving dataframes
all_data_weekly = season_weekly_rushing_df.merge(season_weekly_receiving_df,
                                                left_on=['rusher_player_id', 'Season', 'game_number'],
                                                right_on=['receiver_player_id', 'Season', 'game_number'],
                                                how='outer')

# Merging the above result with the passing dataframe
all_data_weekly = all_data_weekly.merge(season_weekly_passing_df,
                                        left_on=['rusher_player_id', 'Season', 'game_number'],
                                        right_on=['passer_player_id', 'Season', 'game_number'],
                                        how='outer')

# Coalescing the IDs
all_data_weekly['gsis_id'] = all_data_weekly['rusher_player_id'].combine_first(all_data_weekly['receiver_gsis']).combine_first(all_data_weekly['passer_gsis'])


# 1. Replace infinite values with NaN
all_data_weekly.replace([np.inf, -np.inf], np.nan, inplace=True)

# 2. Replace NaN values in numeric columns with 0
numerics = all_data_weekly.select_dtypes(include=[np.number]).columns
all_data_weekly[numerics] = all_data_weekly[numerics].fillna(0)

all_data_weekly['fantasyPoints'] = (all_data_weekly['Total_Yards_x'] * rush_yards +
                                   all_data_weekly['TDs_x'] * rush_td +
                                   (all_data_weekly['Fumbles_x'] + all_data_weekly['Fumbles_y']) * fmb +
                                   all_data_weekly['Total_Yards_y'] * receiving_yards +
                                   all_data_weekly['TDs_y'] * receiving_td +
                                   all_data_weekly['Receptions'] * ppr +
                                   all_data_weekly['Total_Yards'] * pass_yards +
                                   all_data_weekly['TDs'] * pass_td +
                                   all_data_weekly['Interceptions'] * interception)

# Select columns from rosters dataframe
selected_rosters = all_rosters[['gsis_id', 'season', 'team', 'full_player_name', 'position']]

# Left join all_data_weekly with selected columns of rosters
all_data_weekly = pd.merge(all_data_weekly, selected_rosters, how='left',
                           left_on=['gsis_id', 'Season'], right_on=['gsis_id', 'season'])

# Filter rows where full_player_name is not NaN
all_data_weekly = all_data_weekly[all_data_weekly['full_player_name'].notna()]


# Step 1: Filter
weekly_filtered_data = all_data_weekly[all_data_weekly['position'].isin(['RB', 'WR', 'TE', 'QB'])]

# Step 2: Arrange
weekly_filtered_data = weekly_filtered_data.sort_values(by='fantasyPoints', ascending=False)

# Step 3: Group and Step 4-7: Calculate posrank, replacementValue, and PAR
def compute_values(group):
    group['posrank'] = group['fantasyPoints'].rank(ascending=False, method='first')
    conditions = [
        (group['position'] == 'QB') & (group['posrank'] == round(number_of_teams * 1.2)),
        (group['position'] == 'RB') & (group['posrank'] == round(number_of_teams * 3)),
        (group['position'] == 'WR') & (group['posrank'] == round(number_of_teams * 3)),
        (group['position'] == 'TE') & (group['posrank'] == round(number_of_teams * 1.2))
    ]
    #group['replacementValue'] = np.select(conditions, group['fantasyPoints'], default=np.nan)
    group['replacementValue'] = np.select(conditions, [group['fantasyPoints']] * len(conditions), default=np.nan)

    group['PAR'] = group['fantasyPoints'] - group['replacementValue'].max()
    group['PAR'] = group['PAR'].replace([np.inf, -np.inf], 0)
    return group

weekly_grouped_data = weekly_filtered_data.groupby(['Season', 'position', 'game_number']).apply(compute_values)

# Step 8: Select columns
weekly_grouped_data = weekly_grouped_data[['Season', 'gsis_id', 'fantasyPoints', 'PAR']]

# Step 9-10: Group and Summarise
if 'Season' in weekly_grouped_data.index.names:
    weekly_grouped_data.index = weekly_grouped_data.index.droplevel('Season')

# Step 9-10: Group and Summarise
weekly_PAR_summary = weekly_grouped_data.groupby(['Season', 'gsis_id']).agg(
    positive_weekly_PAR=('PAR', lambda x: x[x > 0].sum()),
    weekly_PAR=('PAR', 'sum'),
    sd_PAR=('PAR', 'std'),
    games_played=('PAR', 'size')
).reset_index()


### ADD THE WEEKLY PAR
# Left join all_data_df with weekly_par
all_data_df = pd.merge(all_data_df, weekly_PAR_summary, on=['gsis_id', 'Season'], how='left')

# Replace NaN values in positive_weekly_PAR_summary and games_played columns with 0
all_data_df['positive_weekly_PAR'].fillna(0, inplace=True)
all_data_df['games_played'].fillna(0, inplace=True)

# Remove duplicate rows
all_data_df = all_data_df.drop_duplicates()


### ADD TEAM DATA
# Step 1: Filter pbp_data
filtered_pbp_data = pbp_data[(pbp_data['play_type'] != "No Play") & (pbp_data['posteam'] != 'NA')]

# Step 2: Group by and compute aggregated statistics
team_data = filtered_pbp_data.groupby(['posteam', 'Season']).agg(
    passYards=pd.NamedAgg(column='yards_gained', aggfunc=lambda x: x[filtered_pbp_data['pass_attempt'] == 1].sum()),
    passAttempts=pd.NamedAgg(column='pass_attempt', aggfunc='sum'),
    passTDs=pd.NamedAgg(column='touchdown', aggfunc=lambda x: x[filtered_pbp_data['pass_attempt'] == 1].sum()),
    rushYards=pd.NamedAgg(column='yards_gained', aggfunc=lambda x: x[filtered_pbp_data['rush_attempt'] == 1].sum()),
    rushAttempts=pd.NamedAgg(column='rush_attempt', aggfunc='sum'),
    rushTDs=pd.NamedAgg(column='touchdown', aggfunc=lambda x: x[filtered_pbp_data['rush_attempt'] == 1].sum())
).reset_index()

# Step 3: Left join all_data_df with team_data
all_data_df = pd.merge(all_data_df, team_data, left_on=['team', 'Season'], right_on=['posteam', 'Season'], how='left')


### ADD ROSTER DATA
# Initialize the column with zeros
all_data_df['hasYearBefore'] = 0

# Iterate through each row of the dataframe
for idx, row in all_data_df.iterrows():
    gsis_id = row['gsis_id']
    full_player_name = row['full_player_name']
    next_season = row['Season'] + 1

    # Check if the player has a record in the next season based on gsis_id
    condition_by_id = (all_data_df['Season'] == next_season) & (all_data_df['gsis_id'] == gsis_id)
    count_by_id = len(all_data_df[condition_by_id])

    # Check if the player has a record in the next season based on full_player_name
    condition_by_name = (all_data_df['Season'] == next_season) & (all_data_df['full_player_name'] == full_player_name)
    count_by_name = len(all_data_df[condition_by_name])

    # Update the hasYearBefore column based on the results
    if count_by_id > 0:
        all_data_df.loc[condition_by_id, 'hasYearBefore'] = 1
    elif count_by_id == 0 and count_by_name > 0:
        all_data_df.loc[condition_by_name, 'hasYearBefore'] = 1

def get_next_season_points(row):
    # Get next season and the player's id and name
    next_season = row['Season'] + 1
    gsis_id = row['gsis_id']
    full_player_name = row['full_player_name']

    # Try to find the fantasy points for the next season using gsis_id
    val = all_data_df.loc[
        (all_data_df['Season'] == next_season) & (all_data_df['gsis_id'] == gsis_id),
        'fantasyPoints'
    ].iloc[0] if len(all_data_df[
        (all_data_df['Season'] == next_season) & (all_data_df['gsis_id'] == gsis_id)
    ]) > 0 else None

    # If the above is unsuccessful, try using full_player_name
    if val is None:
        val = all_data_df.loc[
            (all_data_df['Season'] == next_season) & (all_data_df['full_player_name'] == full_player_name),
            'fantasyPoints'
        ].iloc[0] if len(all_data_df[
            (all_data_df['Season'] == next_season) & (all_data_df['full_player_name'] == full_player_name)
        ]) > 0 else None

    return val if val is not None else 0

# Apply the function to each row of the dataframe
all_data_df['nextSeasonsPoints'] = all_data_df.apply(get_next_season_points, axis=1)

def get_next_season_team(row):
    # Get next season and the player's id
    next_season = row['Season'] + 1
    gsis_id = row['gsis_id']

    # Try to find the team for the next season using gsis_id
    team_next_season = all_data_df.loc[
        (all_data_df['Season'] == next_season) & (all_data_df['gsis_id'] == gsis_id),
        'team'
    ]

    return team_next_season.iloc[0] if len(team_next_season) > 0 else "FA"

# Apply the function to each row of the dataframe
all_data_df['nextSeasonsTeam'] = all_data_df.apply(get_next_season_team, axis=1)

# Just a little progress check on the data
print("all_data_df top 25 after adding Fantasy Points, PAR, and next season's points and team!")
print(all_data_df[all_data_df["Season"]>=2017][["Season","full_player_name", "team", "games_played", "positive_weekly_PAR", "weekly_PAR", "fantasyPoints", "fantasy_points_8", "fantasy_points_4", "nextSeasonsPoints", "hasYearBefore"]].head(10))

### Create rows for previous rookies next.
# Step 1: Reordering and filtering columns
columns_to_keep = [col for col in all_data_df.columns if col not in ['Player_Name.x', 'Player_Name.y', 'Player_Name', 'Name', 'rusher_player_id', 'receiver_gsis', 'passer_gsis']]
data = all_data_df[columns_to_keep].sort_values(by="Season")
data['nextSeasonsPoints'] = data['nextSeasonsPoints'].astype(float)

# Step 2: Create new columns
data['firstFullYear'] = data.groupby('gsis_id')['Season'].transform('first') == data['Season']
data['isRookie'] = 0

# Step 3: Capture rookie data
rookies = data[data['firstFullYear'] | (data['hasYearBefore'] == 0)].copy()

# Step 4: Iterate and adjust values
for idx, row in rookies.iterrows():
    name = row['full_player_name']
    gsis_id = row['gsis_id']
    Season = row['Season']
    team = row['team']
    nextSeasonsTeam = row['team']
    nextSeasonsPoints = row['fantasyPoints']

    # Set all columns to 0
    rookies.loc[idx] = 0

    # Update specific columns
    rookies.at[idx, 'gsis_id'] = gsis_id
    rookies.at[idx, 'Season'] = Season - 1
    rookies.at[idx, 'team'] = team
    rookies.at[idx, 'nextSeasonsTeam'] = nextSeasonsTeam
    rookies.at[idx, 'nextSeasonsPoints'] = nextSeasonsPoints
    rookies.at[idx, 'isRookie'] = 1
    rookies.at[idx, 'full_player_name'] = name
    rookies.at[idx, 'position'] = str(rookies.at[idx, 'position'])

# Append to main dataframe
data = pd.concat([data, rookies])

# Step 5: Increment Season
data['Season'] = data['Season'] + 1
data2 = data.copy()


### Cleaning data for next join
