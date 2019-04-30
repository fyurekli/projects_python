import pandas as pd
import os
pd.set_option('display.max_columns',None)
pd.options.display.float_format = '{:.4f}'.format
#----------------------------------------------------------------------------------------------------------------------#
#Import and Append all dataframes
#----------------------------------------------------------------------------------------------------------------------#
'''Get Path Names'''
#----------------------------------------------------------------------------------------------------------------------#
path_name_pbp = []
path_name_box = []
path_players = []
for root, dirs, files in os.walk('nba_analysis/'):
    for file in files:
        if file.startswith("df_box"):
            path_name_box.append(os.path.join(root, file))   
        elif file.startswith("df_pbp"):
            path_name_pbp.append(os.path.join(root, file))
        elif file.startswith('xxx'):
            path_players.append(os.path.join(root, file))
del dirs, file, files, root  
#----------------------------------------------------------------------------------------------------------------------#                          
''' Combine Files '''
#----------------------------------------------------------------------------------------------------------------------#  
df_box = pd.DataFrame()
for f in path_name_box:
    data = pd.read_csv(f)
    df_box = df_box.append(data)
    
df_pbp = pd.DataFrame()
for f in path_name_pbp:
    data = pd.read_csv(f)
    df_pbp = df_pbp.append(data)
 
players = pd.DataFrame()
for f in path_players:
    data = pd.read_csv(f)
    players = players.append(data)    
    
del f, path_name_box, path_name_pbp, data
#----------------------------------------------------------------------------------------------------------------------#
'''Export Data'''
#----------------------------------------------------------------------------------------------------------------------#
df_box.drop(columns='Unnamed: 0', inplace=True)
players.drop(columns='Unnamed: 0', inplace=True)

y = pd.read_csv('nba_analysis/clean_data/df_pbp_2018_2019.csv')


df_box.to_csv('nba_analysis/df_box_2009_2019.csv',index=False)
df_pbp.to_csv('nba_analysis/df_pbp_2012_2019.csv',index=False)
players.to_csv('nba_analysis/df_players.csv',index=False)
#----------------------------------------------------------------------------------------------------------------------#

df = pd.read_csv('bball/pbp_clean_2017.csv')
