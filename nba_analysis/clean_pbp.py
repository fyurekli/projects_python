import pandas as pd
import numpy as np
import string
pd.set_option('display.max_columns',None)
pd.options.display.float_format = '{:.4f}'.format
#----------------------------------------------------------------------------------------------------------------------#
#df_raw = pd.read_csv('nba_analysis/df_pbp_raw.csv')
#df_box = pd.read_csv('nba_analysis/df_box_2009_2019.csv').rename(columns={'links':'link','name':'player'})
#df_players = pd.read_csv('nba_analysis/data/clean_data/combined/df_players_2001_2019.csv')
#links = pd.read_csv('nba_analysis/links.csv')
df_box = pd.read_csv('nba_analysis/data/clean_data/combined/df_box_2009_2019.csv').rename(columns={'links':'link','name':'player'})
df_raw = pd.read_csv('nba_analysis/data/raw_data/combined/pbp_raw_2001_2019.csv')
df_players = pd.read_csv('nba_analysis/data/clean_data/combined/df_players_2001_2019.csv')
links = pd.read_csv('nba_analysis/data/clean_data/combined/links.csv')
#----------------------------------------------------------------------------------------------------------------------#
df_box['link'] = df_box['link'].apply(lambda x: x.split('boxscores/')[1].split('.html')[0])
df_raw['link'] = df_raw['link'].apply(lambda x: x.split('pbp/')[1].split('.html')[0])
links['link'] = links['link'].apply(lambda x: x.split('pbp/')[1].split('.html')[0])
#----------------------------------------------------------------------------------------------------------------------#
#df_raw = df_raw.reset_index().rename(columns={'index':'game_index'})
df_unique = df_raw.drop_duplicates(subset='link').reset_index()
players = df_players['0'].dropna()
players = players.unique()
#-----------------------------------------------------------------------------#
def add_blank_rows(df, column, string=None, index_location=None):
    def add_blanks(df, index):
        added_indeces = [[x + .1, x + .2, x + .3, x + .4] for x in index]
    
        flat_list = []
        for sublist in added_indeces:
            for item in sublist:
                flat_list.append(item)
        flat_list.append(-1)
    
        df_to_append = pd.DataFrame(columns=['text'], index = flat_list)
        df_to_append[column] = None
        df = df.append(df_to_append).sort_index().reset_index(drop=True).drop(0)
        
        return df
    if index_location is None:
        index = df.loc[df[column].str.contains(string)].index.tolist()
        return add_blanks(df, index)
    if string is None:
        index = [index_location]
        return add_blanks(df, index)
#-----------------------------------------------------------------------------#
#df = add_blank_rows(df, 'text', string='Jump')
#-----------------------------------------------------------------------------#
def remove_strings(dataframe, column, *strings_to_remove):
    strings = []
    for item in strings_to_remove:
        strings.append(item)
    dataframe[column].fillna('',inplace=True)
    dataframe[column] = dataframe[column].str.replace('|'.join(strings),'',regex=True)
    dataframe[column] = [x.strip() for x in dataframe[column]]
    return dataframe[column]
#-----------------------------------------------------------------------------#
#add the following string into the dataframe (string + each letter of the alphabet)        
#strings_to_remove = ['/players/' + x + '/' for x in (list(string.ascii_lowercase))]
#strings_to_remove.extend(['colspan="5">','class=','center"','"','<td','td>','<','>','=','bbr-play-score',
#                          'a href','/a','bbr-play-tie','bbr-play-leadchange', '.html','/'])
#strings_to_remove.extend(players)
#strings_to_remove = tuple(strings_to_remove)

#remove_strings(df, 'text', *strings_to_remove)
#-----------------------------------------------------------------------------#
def fix_errors(dataframe):
    if link =='http://www.basketball-reference.com/boxscores/pbp/201803170NOP.html':
        dataframe.drop(804,inplace=True)
    if link =='http://www.basketball-reference.com/boxscores/pbp/201803040SAC.html':
        dataframe.drop(2492,inplace=True)
    if link =='http://www.basketball-reference.com/boxscores/pbp/201812160DEN.html':
        dataframe['text'][497] = dataframe['text'][497] + 'jokicni01'
    if link =='http://www.basketball-reference.com/boxscores/pbp/201404210OKC.html':
        dataframe['text'][2689] = 'koufoko01 enters the game for gasolma01'
        dataframe['text'][2719] = dataframe['text'][2719] + 'koufoko01'
    if link =='http://www.basketball-reference.com/boxscores/pbp/201502040ATL.html':
        dataframe['text'][2685] = 'butlera01 R. Butler enters the game for walljo01'
        dataframe['text'][2695] = 'jenkijo01 J. Jenkins enters the game for carrode01'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201312020SAS.html':
        dataframe['text'][2493] = 'anticpe01 enters the game for carrode01 D. Carroll'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201401130NYK.html':
        dataframe['text'][3031] = 'lenal01 enters the game for tuckepj01'
        dataframe['text'][3049] = ' tuckepj01 P. Tucker enters the game for lenal01'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201404190LAC.html':
        dataframe['text'][1687] = 'kuzmiog01 enters the game for speigma01 M. Speights'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201411130MEM.html':
        dataframe['text'][2721] = 'holliry01 enters the game for mclembe01 B. McLemore'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201404010DAL.html':
        dataframe['text'][2587] = 'armsthi01 enters the game for thompkl01 K. Thompson'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201402120GSW.html':
        dataframe['text'][2527] = 'kuzmiog01 enters the game for greendr01 D. Green'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201402080PHO.html':
        dataframe['text'][1435] = 'kuzmiog01 enters the game for iguodan01'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201411120PHO.html':
        dataframe['text'][2745] = 'jeffeco01 enters the game for johnsjo02'
    if link=='http://www.basketball-reference.com/boxscores/pbp/201501060SAS.html':
        dataframe['text'][3021] = 'anthojo01 enters the game for jennibr01'
    #if link=='http://www.basketball-reference.com/boxscores/pbp/201211260LAC.html':
    #    dataframe = add_blank_rows(df_4, 'text', 'Official timeout')      
    #if link=='http://www.basketball-reference.com/boxscores/pbp/201203160LAL.html':
    #    dataframe['text'].fillna('',inplace=True)
    #    dataframe = add_blank_rows(df_3, 'text', 'full timeout')      
    if link=='http://www.basketball-reference.com/boxscores/pbp/201101220NJN.html':
        dataframe['text'].fillna('',inplace=True)
        dataframe = add_blank_rows(dataframe, 'text', index_location=2433)
        dataframe.reset_index()
    return dataframe

#df = fix_errors(df)
#-----------------------------------------------------------------------------#
def add_quarter(dataframe, column, new_column, *values):
    dataframe[new_column] = None
    to_remove = []
    for value in values: 
        period = value
        value = 'Start of ' + period
        to_remove.extend([value])
        dataframe[new_column] = np.where((dataframe[column].str.contains(value)) & (dataframe[new_column].isnull()),
                                          period, dataframe[new_column])
        value = 'End of'
        dataframe[new_column] = np.where(dataframe[column].str.contains(value),'delete', dataframe[new_column])  
    #if the jump_ball isn't there and it says 'start of 1st quarter'
    if dataframe[new_column].iloc[1] and 'Start of 1st' in dataframe[new_column].iloc[1]:
        dataframe[new_column].loc[dataframe.index[dataframe[new_column].str.contains('1st quarter')==True]+1] = '1st quarter'
    else:
        dataframe[new_column].iloc[0] = '1st quarter'
    index = list(dataframe.loc[dataframe[new_column]=='delete'].index-2)
    dataframe[new_column].iloc[index] = 'delete'
    dataframe[new_column].ffill(inplace=True)
    #in case text containing 'Start of 1st quarter'
    index_delete = dataframe[dataframe[column].str.contains('Start of 1st quarter')==True].index
    dataframe.drop(index_delete,inplace=True)
    dataframe.drop(index_delete-1,inplace=True)
    #get rid of some last stuff
    dataframe[new_column] = np.where(dataframe[column].str.contains('|'.join(to_remove)), 'delete', dataframe[new_column])
    dataframe = dataframe[dataframe[new_column] != 'delete']

    dataframe1 = pd.DataFrame(np.array(dataframe['text']).reshape(-1,6),
                                      columns = ['time','home_play','points','score','points','away_play'])
    dataframe2 = pd.DataFrame(np.array(dataframe['period']).reshape(-1,6),
                                      columns = ['time','home_play','points','score','points','away_play'])
    dataframe2 = pd.DataFrame(dataframe2['time']).rename(columns={'time':'period'})
    dataframe = pd.concat([dataframe1, dataframe2],axis=1)
    
    return dataframe

#periods = ('2nd quarter','3rd quarter','4th quarter','1st overtime','2nd overtime','3rd overtrime',
 #          '4th overtime','5th overtime','6th overtime')

#df = add_quarter(df, 'text', 'period', *periods)
#-----------------------------------------------------------------------------#
def add_team_subs(dataframe):
    dataframe['team'] = np.where(dataframe['home_play'].str.contains('by Team'),home_team,
                        np.where(dataframe['away_play'].str.contains('by Team'),away_team,
                           None))
    dataframe['play'] = dataframe['home_play'] + dataframe['away_play']
    dataframe = dataframe[['time','period','play','score','team']]
    #substitution column add-on
    dataframe['enters_game'] = np.where(dataframe['play'].str.contains('enters the game for'), 
                                        dataframe['play'].apply(lambda x: x.split(' enters ')[0]), 
                                        None)
    dataframe['exits_game'] = np.where(dataframe['play'].str.contains('enters the game for'), 
                                       dataframe['play'].apply(lambda x: x.split(' game for ')[1] if 'enters' in x else None), 
                                       None)            
    dataframe['enters_game'] = dataframe['enters_game'].str.strip()
    dataframe['exits_game'] = dataframe['exits_game'].str.strip()
    
    return dataframe


#df = add_team_subs(df)
#delete later
#df['exits_game'].fillna('',inplace=True)
#df['enters_game'].fillna('',inplace=True)
#df['exits_game'] = df['exits_game'].apply(lambda x: x.split('.')[0])
#df['exits_game'] = [x[:-1] if 'Team' not in x else x for x in df['exits_game']]
#df['enters_game'] = df['enters_game'].apply(lambda x: x.split('.')[0])
#df['enters_game'] = [x[:-1] if 'Team' not in x else x for x in df['enters_game']]     
#df.replace('', np.nan, inplace=True) 
#-----------------------------------------------------------------------------#
plays = ('Defensive rebound',
         'Offensive rebound',
         'Turnover',
         'Def 3 sec tech',
         'foul',
         'ejected',
         'enters',
         'full timeout',
         'official timeout',
         '20 second timeout', 
         '20 sec timeout',
         'makes 2-pt', 
         'misses 2-pt',
         'makes 3-pt', 
         'misses 3-pt',
         'makes clear path free throw', 
         'misses clear path free throw',
         'makes flagrant free throw', 
         'misses flagrant free throw',
         'makes technical free throw', 
         'misses technical free throw',
         'makes free throw', 
         'misses free throw',
         '3 sec tech',
         'def goaltending',
         'defensive goaltending',
         'vs.')

secondary_plays = ('assist',
                   'drawn',
                   'block',
                   'steal',
                   'enters the game for',
                   'vs.',
                   'Jump ball')

play_details = ('lost ball',
                'offensive foul',
                'bad pass',
                'out of bounds',
                'shot clock',
                'discontinued dribble',
                'traveling',
                'illegal assist',
                'back court',                
                '3 sec',              
                'offensive goaltending',
                'off goaltending',
                'dbl dribble',
                'palming',              
                '5 sec',
                'illegal screen',
                'lane violation',
                'jump ball violation',     
                '8 sec',
                'swinging elbows',          
                'punched ball',
                'score in opp',
                'inbound',
                'excessive timeout',
                'jump shot',
                'layup',
                'dunk',
                'hook shot',
                'tip-in',
                '1 of 1', '1 of 2', '2 of 2', '1 of 3', '2 of 3', '3 of 3',
                'Shooting foul',
                'Personal foul',
                'Personal block foul',
                'Shooting block foul',
                'Offensive foul',
                'Offensive charge foul',
                'Loose ball foul',
                'Away from play foul',
                'Personal take foul',
                'Technical foul',
                'Flagrant foul type 1',
                'Flagrant foul type 2')      

#-----------------------------------------------------------------------------#            
def pull_players(dataframe, column, new_column, *values, data='primary'):
    if data is 'primary':
        dataframe[new_column] = None
        for value in values:
            try:
                conditional = dataframe[column].str.contains(value) & dataframe[new_column].isnull()
                condition_met = dataframe[conditional][column].apply(lambda x: 
                    x.split(value + ' by')[1].split('(')[0] if value + ' by' in x and '(' in x else 
                    x.split(value + ' by')[1] if value + ' by ' in x else
                    x.split(value)[0] if value in x else  
                    None)

                condition_not_met = dataframe[~conditional][new_column]
                dataframe[new_column] = pd.concat([condition_met, condition_not_met]).sort_index() 
            except:
                print(value)
                continue
            
    if data is 'secondary':
        dataframe[new_column] = None
        for value in values:
            try:
                conditional = dataframe[column].str.contains(value) & dataframe[new_column].isnull()
                condition_met = dataframe[conditional][column].apply(lambda x: 
                    x.split(value + ' by')[1].split(')')[0] if value + ' by' in x and ')' in x else 
                    x.split(value)[1].split('(')[0] if value in x and '(' in x else
                    x.split(value + ' by')[1] if value + ' by ' in x else
                    x.split(value)[1] if value in x else  
                    None)

                condition_not_met = dataframe[~conditional][new_column]
                dataframe[new_column] = pd.concat([condition_met, condition_not_met]).sort_index() 
            except:
                print(value)
                continue          
    return dataframe[new_column]
#-----------------------------------------------------------------------------#
def pull_plays(dataframe, column, new_column, *values):
    dataframe[new_column] = None
    for value in values:
        dataframe[new_column] = np.where((dataframe[column].str.contains(value) & dataframe[new_column].isnull()),
                                          value.lower(), dataframe[new_column])
    return dataframe[new_column]
#-----------------------------------------------------------------------------#
def pull_distance(dataframe, column, new_column):
    dataframe[new_column] = dataframe[column].apply(lambda x:
                                x.split('from ')[1].split(' ft')[0] if 'from' in x else
                                x.split('at ')[1].split(' (')[0] if 'at rim' in x else
                                None).str.replace('rim','0')
    return dataframe
#-----------------------------------------------------------------------------#
def pull_data(dataframe):
    pull_plays(dataframe, 'play', 'play_1', *plays)
    pull_players(dataframe, 'play', 'player', *plays, data='primary')
    
    pull_plays(dataframe, 'play', 'play_details', *play_details)
    dataframe['player'] = dataframe['player'].str.replace('Jump ball:', '')
    df1 = dataframe.copy()
    
    pull_plays(df1, 'play', 'play_1', *secondary_plays)
    pull_players(df1, 'play', 'player', *secondary_plays, data='secondary')
    df1['play_1'] = df1['play_1'].str.replace('enters the game for', 'exits')
    
    df = dataframe.append(df1)
    df = df.sort_index()
    
    df['play_1'] = df['play_1'].str.replace('vs.', 'jump ball')
    df['play_1'] = df['play_1'].str.replace('drawn', 'fouled')
    pull_distance(df, 'play', 'distance')
    df.dropna(subset=['player'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.rename(columns={'play_1':'play','play':'play_raw'})
    df['player'] = df['player'].apply(lambda x: x.strip())
    df['play_index'] = df.index
    return df

#df = pull_data(df)
#-----------------------------------------------------------------------------#
def fix_score(dataframe):
    #fix blanks introduced by jump_ball
    dataframe['score'] = [None if x == '' else x for x in dataframe['score']]
    dataframe['score'].fillna(method='ffill',inplace=True)
    dataframe['score'].fillna(method='bfill',inplace=True)
    dataframe['score_home'] = dataframe['score'].apply(lambda x: int(x.split('-')[0]))
    dataframe['score_away'] = dataframe['score'].apply(lambda x: int(x.split('-')[1]))
    dataframe['score_diff'] = dataframe['score_home'] - dataframe['score_away']
    return dataframe

#fix_score(df)
#-----------------------------------------------------------------------------#
def final_fixes(dataframe):
    dataframe = dataframe[['play_index','period','time','score','score_home','score_away', 'score_diff',
                           'play_raw','player','team','play','distance','play_details','enters_game','exits_game']]
    dataframe['date'] = date
    dataframe['link'] = link
    return dataframe

#df = final_fixes(df)
#delete:
#df['player'] = df['player'].apply(lambda x: x.split('.')[0])
#df['player'] = [x[:-1] if 'Team' not in x else x for x in df['player']]
#-----------------------------------------------------------------------------#
def get_starters(dataframe):
    #Designate Starters and Bench Players
    #First five of each box score game is a starter, rest are bench players
    dataframe = dataframe[dataframe['link']==link]
    dataframe['changed'] = dataframe['team_key'].ne(dataframe['team_key'].shift())
    changed_indeces = dataframe.loc[dataframe['changed']==True].index.tolist()
    changed_indeces = [[x, x + 1, x + 2, x + 3, x + 4] for x in changed_indeces]
    #flatten the list of lists into one long list
    starter_index = []
    for sublist in changed_indeces:
        for item in sublist:
            starter_index.append(item)        
    starters = pd.DataFrame(columns=['role'], index = starter_index, data='starter')
    dataframe = pd.concat([dataframe,starters],axis=1,join_axes=[dataframe.index])
    dataframe['role'].fillna('bench',inplace=True)    
    dataframe.reset_index(drop=True,inplace=True)
    return dataframe

#box = get_starters(df_box)
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
#adding teams to df_pbp dataframe
def merge_dfs(dataframe, dataframe2):
    dataframe = pd.merge(dataframe, dataframe2[['player','team','link']], how='left',on=['link','player'])
    dataframe['team_x'].fillna('',inplace=True)
    dataframe['team_y'].fillna('',inplace=True)
    dataframe['team'] = dataframe['team_y'] + dataframe['team_x']
    dataframe.drop(columns=['team_x','team_y'],inplace=True)
    return dataframe

#df = merge_dfs(df, box)
#-----------------------------------------------------------------------------#     
#adding 'home'/'away' to df_pbp dataframe
#getting the 'away'/'home' status of the game in order to merge it with the df_pbp dataframe
#the reason this couldn't be merged above is because we had to include player, and 'Team' wasn't a variable in the df_box df
def add_home_away(dataframe):
    away_home_df = box.groupby(['link','team'])['team_key'].unique().reset_index()
    away_home_df['team_key'] = away_home_df['team_key'].apply(lambda x: "".join(x))
    dataframe = pd.merge(dataframe, away_home_df, how='left',on=['link','team'])
    return dataframe

#df = add_home_away(df)
#-----------------------------------------------------------------------------#
 
def add_starters(dataframe):
    starters = box[box['role']=='starter']
    starters = starters.groupby(['link','team'])['player'].unique().reset_index().rename(columns={'player':'starters'})
    
    dataframe = pd.merge(dataframe, starters, on=['link','team'])
    dataframe['changed'] = dataframe['team'].ne(dataframe['team'].shift())
    
    changed_indeces = dataframe.loc[dataframe['changed']==False].index.tolist()
    dataframe['starters'].loc[changed_indeces] = np.nan
    
    dataframe.drop(columns=['changed'],inplace=True)
    dataframe = pd.concat([dataframe, dataframe['starters'].dropna().apply(lambda x: 
                           x.tolist()).apply(lambda x: ','.join(x)).str.split(',', expand=True)],axis=1,join_axes=[dataframe.index])
    return dataframe

#df = add_starters(df)
#-----------------------------------------------------------------------------#
#filling in who is in the game and when
def fill_it_up(dataframe, *columns):
    dataframe.columns = dataframe.columns.astype(str)
    for column in columns:
        for i in range(dataframe['exits_game'].notnull().sum()//2+1):
            try:
                not_null = dataframe[dataframe[column].notnull()].index[i]
                string = dataframe[column].loc[not_null]
                index = dataframe[(dataframe['play'] == 'exits') & (dataframe['player'].str.contains(string))].loc[not_null:].index[0]
                dataframe[column].iloc[index] = dataframe['enters_game'][index]
            except:
                pass
        dataframe[column].fillna(method='ffill',inplace=True)
    return dataframe[column]
    
#columns = ('0','1','2','3','4')
#fill_it_up(df, *columns)
#-----------------------------------------------------------------------------#
def last_step(dataframe):
    dataframe['starters'] = dataframe['0'] + ', ' + dataframe['1'] + ', ' + dataframe['2'] + ', ' + \
                            dataframe['3'] + ', ' + dataframe['4']
    dataframe.sort_index(inplace=True)
    #Combine links dataframe with main dataframe
    dataframe = pd.merge(dataframe,links,how='left',on='link')
    return dataframe

#last_step(df)
#-----------------------------------------------------------------------------#
def change_time(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    #convert time to datetime format
    dataframe['time'] = ['0' + x if len(x) < 7 else x for x in dataframe['time']]
    dataframe['time'] = [x + '000' for x in dataframe['time']]
    dataframe['time'] = pd.to_datetime(dataframe['time'], format="%M:%S.%f")
    return dataframe

#change_time(df)
#-----------------------------------------------------------------------------#
df_all = pd.DataFrame()
df_errors = pd.DataFrame()
for link in df_unique['link'][24465:]:
    try:
        df = df_raw[df_raw['link']==link]
        df = df.reset_index(drop=True)
        home_team = df['home_away'][0].split(',')[0]
        away_team = df['home_away'][0].split(', ')[1]
        date = df['date'][0]
        df = pd.DataFrame(df,columns=['text']).reset_index(drop=True)
        df[df['text'].str.contains('timeout',na=False)] = df[df['text'].str.contains('timeout',na=False)].apply(lambda x: x + ' by Team')
        #-----------------------------------------------------------------------------#
        df = add_blank_rows(df, 'text', string='Jump')
        #-----------------------------------------------------------------------------#
        strings_to_remove = ['/players/' + x + '/' for x in (list(string.ascii_lowercase))]
        strings_to_remove.extend(['colspan="5">','class=','center"','"','<td','td>','<','>','=','bbr-play-score',
                                  'a href','/a','bbr-play-tie','bbr-play-leadchange', '.html','/'])
        strings_to_remove.extend(players)
        strings_to_remove = tuple(strings_to_remove)
        remove_strings(df, 'text', *strings_to_remove)
        #-----------------------------------------------------------------------------#
        df = fix_errors(df)
        #-----------------------------------------------------------------------------#
        periods = ('2nd quarter','3rd quarter','4th quarter','1st overtime','2nd overtime','3rd overtrime',
                   '4th overtime','5th overtime','6th overtime')
    
        df = add_quarter(df, 'text', 'period', *periods)
        #-----------------------------------------------------------------------------#
        df = add_team_subs(df)
        #delete later
        #df['exits_game'].fillna('',inplace=True)
        #df['enters_game'].fillna('',inplace=True)
        #df['exits_game'] = df['exits_game'].apply(lambda x: x.split('.')[0])
        #df['exits_game'] = [x[:-1] if 'Team' not in x else x for x in df['exits_game']]
        #df['enters_game'] = df['enters_game'].apply(lambda x: x.split('.')[0])
        #df['enters_game'] = [x[:-1] if 'Team' not in x else x for x in df['enters_game']]     
        #df.replace('', np.nan, inplace=True) 
        #-----------------------------------------------------------------------------#
        df = pull_data(df)
        #-----------------------------------------------------------------------------#
        fix_score(df)
        #-----------------------------------------------------------------------------#
        df = final_fixes(df)
        #delete:
        #df['player'] = df['player'].apply(lambda x: x.split('.')[0])
        #df['player'] = [x[:-1] if 'Team' not in x else x for x in df['player']]
        #-----------------------------------------------------------------------------#
        box = get_starters(df_box)
        df = merge_dfs(df, box)
        df = add_home_away(df)
        df = add_starters(df)
        columns = ('0','1','2','3','4')
        fill_it_up(df, *columns)
        last_step(df)
        change_time(df)
        #-----------------------------------------------------------------------------#
        print(str(df_unique[df_unique['link']==link].index[0]) + ' out of ' + str(len(df_unique)))
        df_all = df_all.append(df)
    except:
        df_error = pd.DataFrame(data = {'row':[df_unique[df_unique['link']==link].index[0]],
                                        'link': [link],
                                        'error':'some_error'})
        df_errors = df_errors.append(df_error)
        print(link + 'error')
        continue 
#-----------------------------------------------------------------------------#
df_all.sort_values(by=['link','play_index'], inplace=True)
