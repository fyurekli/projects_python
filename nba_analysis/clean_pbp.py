import pandas as pd
import numpy as np
import string
from tqdm import tqdm
from collections import Counter
import logging
pd.set_option('display.max_columns',None)
pd.options.display.float_format = '{:.4f}'.format
logging.basicConfig(filename = 'nba_pbp_errors.log', level = logging.DEBUG)
#----------------------------------------------------------------------------------------------------------------------#
#df_raw = pd.read_csv('nba_analysis/df_pbp_raw.csv')
#df_box = pd.read_csv('nba_analysis/df_box_2009_2019.csv').rename(columns={'links':'link','name':'player'})
#df_players = pd.read_csv('nba_analysis/data/clean_data/combined/df_players_2001_2019.csv')
#links = pd.read_csv('nba_analysis/links.csv')
df_box = pd.read_csv('nba_analysis/data/df_box_scores_2000_2018_clean.csv').rename(columns={'links':'link','name':'player'})
df_pbp_raw = pd.read_csv('nba_analysis/data/df_pbp_2001_2019_raw.csv')
players = pd.read_csv('nba_analysis/data/df_players_2001_2019_clean.csv')
links = pd.read_csv('nba_analysis/data/df_links_1979_2019_clean.csv')
playoffs = pd.read_csv('nba_analysis/data/df_playoff_dates_1959_2019_clean.csv').rename(columns={'playoff_dates':'playoff_date'})
#----------------------------------------------------------------------------------------------------------------------#
#df_box['team_key'] = np.where(df_box['team_key']=='home', 'away', 'home')
#df_box.to_csv('df_box_scores_2000_2018_clean.csv')
#----------------------------------------------------------------------------------------------------------------------#
playoffs['playoff_date'] = pd.to_datetime(playoffs['playoff_date'])
#fix links
df_box['link'] = df_box['link'].apply(lambda x: x.split('boxscores/')[1].split('.html')[0])
df_pbp_raw['link'] = df_pbp_raw['link'].apply(lambda x: x.split('pbp/')[1].split('.html')[0])
links['link'] = links['link'].apply(lambda x: x.split('pbp/')[1].split('.html')[0])
#----------------------------------------------------------------------------------------------------------------------#
def fix_players(dataframe):
    dataframe = dataframe['0'].dropna().unique()
    return dataframe
#-----------------------------------------------------------------------------#
def find_duplicates(dataframe1, dataframe2, column):
    dataframe1 = dataframe1[column].unique().tolist()
    dataframe2 = dataframe2[column].unique().tolist()
    dfs = dataframe1 + dataframe2
    counts = Counter(dfs)
    duplicates = list(set([df for df in dfs if counts[df] > 1]))
    return duplicates
#-----------------------------------------------------------------------------#
def get_iterable_df(dataframe1, dataframe2, column):
    duplicates = find_duplicates(dataframe1, dataframe2, 'link')
    dataframe1 = dataframe1[dataframe1['link'].isin(duplicates)]
    return dataframe1
#-----------------------------------------------------------------------------#
def get_unique(dataframe1):
    df_unique = dataframe1.drop_duplicates(subset='link').reset_index()
    df_unique = pd.DataFrame(df_unique['link'],columns=['link'])
    return df_unique
#-----------------------------------------------------------------------------#
def missing_dfs(dataframe1, dataframe2):
    dataframe1 = pd.DataFrame(get_unique(dataframe1), columns=['link'])
    dataframe2 = pd.DataFrame(get_unique(dataframe2), columns=['link'])
    res = pd.merge(dataframe1, dataframe2, how='outer', indicator=True)
    res['df_pbp'] = res['link'][res['_merge'] != 'right_only']
    res['df_box'] = res['link'][res['_merge'] != 'left_only']
    #res.drop(['link', '_merge'], axis=1,inplace=True)
    return res
#-----------------------------------------------------------------------------#
players = fix_players(players)
missing_df = missing_dfs(df_pbp_raw, df_box)
df_pbp_raw = get_iterable_df(df_pbp_raw, df_box, 'link')
df_box = get_iterable_df(df_box, df_pbp_raw, 'link')
df_unique = get_unique(df_pbp_raw)
#-----------------------------------------------------------------------------#
def add_blank_rows(df, column, string=None, index_location=None, isblank=False):
    def add_blanks(df, index):
        if isblank == True:
            added_indeces = [[x + .1, x + .2, x + .3, x + .4, x + .5] for x in index]
        else:
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
    if index_location is None and isblank==False:
        index = df.loc[df[column].str.contains(string)].index.tolist()
        return add_blanks(df, index)
    if string is None and isblank == False:
        index = [index_location]
        return add_blanks(df, index)
    if isblank==True:
        index = [index_location]
        return add_blanks(df, index)
#-----------------------------------------------------------------------------#
def remove_strings(dataframe, column, *strings_to_remove):
    dataframe[column] = dataframe[column].str.replace('.html">',' ')
    dataframe[column] = dataframe[column].str.replace('L. Mbah a Moute','')
    dataframe[column] = dataframe[column].str.replace('M. World Peace','')
    dataframe[column] = dataframe[column].str.replace('N. De Colo', '')
    dataframe[column] = dataframe[column].str.replace('III','')
    dataframe[column] = dataframe[column].str.replace('II','')
    strings = []
    for item in strings_to_remove:
        strings.append(item)
        
    dataframe[column].fillna('',inplace=True)
    dataframe[column] = dataframe[column].str.replace('|'.join(strings),'',regex=True)
    
    dataframe[column] = dataframe[column].str.replace('. ','.', regex=False)
    dataframe[column] = dataframe[column].str.replace('vs.','vs. ', regex=False)
    dataframe[column] = dataframe[column].apply(lambda x: ' '.join(['' if '.' in word and word[0].isupper() else word for word in x.split()]))
    dataframe[column] = dataframe[column].str.replace('  ',' ').str.strip()
    dataframe[column] = [x + ')' if '(' in x and ')' not in x else x for x in dataframe[column]]
    
    return dataframe[column]
#-----------------------------------------------------------------------------#
def get_possession(dataframe):
    if link == '200906110ORL':
        dataframe.loc[1:14, 'text'] = '12:00.0','Start of 1st quarter','12:00.0','Jump ball: howardw01 vs. bynuman01','','','','','12:00:0','','','0-0','','Violation by howardw01 (jump ball)'
        
    if link == '200901090NOH':
        dataframe.loc[1:14, 'text'] = '12:00.0','Start of 1st quarter','12:00.0','Jump ball: chandty01 vs. cambyma01','','','Violation by cambyma01 (jump ball)','','12:00:0','','','0-0','',''
        
    if dataframe['text'][0:5].str.contains('Jump').any():
        if dataframe['text'][10] == '':
            possession = 'away'
        elif dataframe['text'][14] == '':
            possession = 'home'
    elif 'Jump' not in dataframe['text'][0:5]:
        logging.info(str(link) + ' no jump ball in first row') #checking for situations where this happens
        if dataframe['text'][4] == '':
            possession = 'away'
        elif dataframe['text'][8] == '':
            possession = 'home'   
            
    return possession
#-----------------------------------------------------------------------------#
def get_overtime_possessions(dataframe, overtime):
    if dataframe['text'].str.contains(overtime).any():
        index = dataframe[dataframe['text'].str.contains(overtime)].index[0]
        if 'Jump' in dataframe['text'][index + 2]:
            if dataframe['text'][index + 8] == '':
                overtime_possession = 'home'
            else:
                overtime_possession = 'away'
            return overtime_possession
        elif 'Jump' not in dataframe['text'][index + 2]:
            logging.info(str(link) + ' no jump ball in overtime row')
            if dataframe['text'][index + 2] == '':
                overtime_possession = 'home'
            else:
                overtime_possession = 'away'
            return overtime_possession
#-----------------------------------------------------------------------------#
def fix_errors(dataframe, link):
    if link =='201803170NOP':
        dataframe = add_blank_rows(dataframe, 'text', isblank=True, index_location=805)
        dataframe.loc[810, 'text'] = 'Technical foul by rondora01' 
        dataframe['text'].fillna('',inplace=True)
    if link =='201803040SAC':
        dataframe = add_blank_rows(dataframe, 'text', isblank=True, index_location=2493)
        dataframe.loc[2498, 'text'] = 'Technical foul by randoza01' 
        dataframe['text'].fillna('',inplace=True)     
    if link =='201812160DEN':
        dataframe['text'][497] = dataframe['text'][497] + 'jokicni01'
    if link =='201404210OKC':
        dataframe['text'][2690] = 'koufoko01 enters the game for gasolma01'
        dataframe['text'][2720] = dataframe['text'][2720] + ' koufoko01'
    if link =='201502040ATL':
        dataframe['text'][2686] = 'butlera01 R. Butler enters the game for walljo01'
        dataframe['text'][2696] = 'jenkijo01 J. Jenkins enters the game for carrode01'
    if link=='201312020SAS':
        dataframe['text'][2494] = 'anticpe01 enters the game for carrode01 D. Carroll'
    if link=='201401130NYK':
        dataframe['text'][3032] = 'lenal01 enters the game for tuckepj01'
        dataframe['text'][3050] = ' tuckepj01 P. Tucker enters the game for lenal01'
    if link=='201404190LAC':
        dataframe['text'][1688] = 'kuzmiog01 enters the game for speigma01 M. Speights'
    if link=='201411130MEM':
        dataframe['text'][2722] = 'holliry01 enters the game for mclembe01 B. McLemore'
    if link=='201404010DAL':
        dataframe['text'][2588] = 'armsthi01 enters the game for thompkl01 K. Thompson'
    if link=='201402120GSW':
        dataframe['text'][2528] = 'kuzmiog01 enters the game for greendr01 D. Green'
    if link=='201402080PHO':
        dataframe['text'][1436] = 'kuzmiog01 enters the game for iguodan01'
    if link=='201411120PHO':
        dataframe['text'][2746] = 'jeffeco01 enters the game for johnsjo02'
    if link=='201501060SAS':
        dataframe['text'][3022] = 'anthojo01 enters the game for jennibr01'
    if link=='201211260LAC':
        dataframe = add_blank_rows(dataframe, 'text', index_location=1810)    
        dataframe['text'].fillna('',inplace=True)
    if link=='201203160LAL':
        dataframe['text'][1572] = 'LA Lakers full timeout by Team'
        dataframe = add_blank_rows(dataframe, 'text', index_location=1571)     
        dataframe['text'].fillna('',inplace=True)
    if link=='201101220NJN':
        dataframe = add_blank_rows(dataframe, 'text', index_location=2434)
        dataframe['text'].fillna('',inplace=True)
    if link=='201612030GSW':
        dataframe.loc[1656, 'text'] = 'lenal01 enters the game for bendedr01'
        dataframe.loc[1662, 'text'] = 'bendedr01 enters the game for chandty01'
    if link=='201702100MIL':
        dataframe.loc[418, 'text'] = 'clarkjo01 enters the game for denglu01'
        dataframe.loc[442, 'text'] = 'denglu01 enters the game for randlju01'      
    if link=='201611020LAC':
        dataframe.loc[3, 'text'] = '12:00.0'
        dataframe.loc[4, 'text'] = 'Jump ball: adamsst01 vs. jordade01 (paulch01 gains possession)'     
        dataframe.loc[8, 'text'] = ''  
        dataframe.loc[9, 'text'] = '11:34.0'
        dataframe.loc[10, 'text'] = ''        
        dataframe.loc[14, 'text'] = 'paulch01 misses 3-pt jump shot from 26 ft'         
    return dataframe
#-----------------------------------------------------------------------------#
#dataframe = df.copy()
#column = 'text'
#new_column = 'period'
#values = '2nd quarter','3rd quarter','4th quarter','1st overtime','2nd overtime','3rd overtime','4th overtime','5th  overtime','6th overtime'
#t = dataframe[dataframe['period'] == '2nd quarter']
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
                                      columns = ['time','away_play','points','score','points','home_play'])
    dataframe2 = pd.DataFrame(np.array(dataframe['period']).reshape(-1,6),
                                      columns = ['time','away_play','points','score','points','home_play'])
    dataframe2 = pd.DataFrame(dataframe2['time']).rename(columns={'time':'period'})
    dataframe = pd.concat([dataframe1, dataframe2],axis=1)
    
    return dataframe
#-----------------------------------------------------------------------------#
def add_team_subs(dataframe, home_team, away_team):
    dataframe['team'] = np.where(dataframe['home_play'].str.contains('by Team'),home_team,
                        np.where(dataframe['away_play'].str.contains('by Team'),away_team,
                           None))
    dataframe['play'] = dataframe['home_play'].copy() + dataframe['away_play'].copy()
    dataframe = dataframe[['time','period','play','score','team']].copy()
    #substitution column add-on
    dataframe['enters_game'] = np.where(dataframe['play'].str.contains('enters the game for'), 
                                        dataframe['play'].apply(lambda x: x.split(' enters ')[0]), 
                                        None)
    dataframe['exits_game'] = np.where(dataframe['play'].str.contains('enters the game for'), 
                                       dataframe['play'].apply(lambda x: x.split(' game for ')[1] if 'enters' in x else None), 
                                       None)            
    dataframe['enters_game'] = dataframe['enters_game'].str.strip().copy()
    dataframe['exits_game'] = dataframe['exits_game'].str.strip().copy()
    
    return dataframe
#-----------------------------------------------------------------------------#
#test = df_pbp_raw[df_pbp_raw['text'].str.contains('Violation')]
plays = ('Defensive rebound','Offensive rebound','Turnover','foul',
         'Def 3 sec tech',
         '3 sec tech'
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
         'vs.',
         'Violation')

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
                'illegal defense',
                'back court',                
                '3 sec',              
                'def goaltending',
                'defensive goaltending',                
                'offensive goaltending',
                'off goaltending',
                'kicked ball',
                'delay of game',
                'double dribble',
                'dbl dribble',
                'palming',              
                '5 sec',
                'illegal screen',
                'lane violation',
                'lane',
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
    dataframe['play_index'] = dataframe.index
    
    df1 = dataframe.copy()
    
    pull_plays(df1, 'play', 'play_1', *secondary_plays)
    pull_players(df1, 'play', 'player', *secondary_plays, data='secondary')
    df1['play_1'] = df1['play_1'].str.replace('enters the game for', 'exits')
    df1['play_index'] = df1.index
    
    df = dataframe.append(df1)
    df = df.sort_index()
    
    df['play_1'] = df['play_1'].str.replace('vs.', 'jump ball')
    df['play_1'] = df['play_1'].str.replace('drawn', 'fouled')
    pull_distance(df, 'play', 'distance')
    df.dropna(subset=['player'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.rename(columns={'play_1':'play','play':'play_raw'})
    df['player'] = df['player'].apply(lambda x: x.strip())
    
    return df
#-----------------------------------------------------------------------------#
def fix_score(dataframe):
    #fix blanks introduced by jump_ball
    dataframe['score'] = [None if x == '' else x for x in dataframe['score']]
    dataframe['score'] = dataframe['score'].fillna(method='ffill')
    dataframe['score'] = dataframe['score'].fillna(method='bfill')
    dataframe['score_home'] = dataframe['score'].apply(lambda x: int(x.split('-')[0]))
    dataframe['score_away'] = dataframe['score'].apply(lambda x: int(x.split('-')[1]))
    dataframe['score_diff'] = dataframe['score_home'] - dataframe['score_away']
    return dataframe
#-----------------------------------------------------------------------------#
def final_fixes(dataframe, date, link):
    dataframe = dataframe[['play_index','period','time','score','score_home','score_away', 'score_diff',
                           'play_raw','player','team','play','distance','play_details','enters_game','exits_game']].copy()
    dataframe['date'] = date
    dataframe['link'] = link
    return dataframe
#-----------------------------------------------------------------------------#
def get_starters(dataframe, link):
    #Designate Starters and Bench Players
    #First five of each box score game is a starter, rest are bench players
    dataframe = dataframe[dataframe['link']==link].copy()
    dataframe['changed'] = dataframe['team_key'].ne(dataframe['team_key'].shift()).copy()
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
    if dataframe.shape[0] > 40:
        logging.info(str(link) + 'box_score was not filtered')
        dataframe.drop_duplicates(subset=['player'],inplace=True)
    return dataframe
#-----------------------------------------------------------------------------#
#adding teams to df_pbp dataframe
def merge_dfs(dataframe, dataframe2):
    dataframe = pd.merge(dataframe, dataframe2[['player','team','link']], how='left',on=['link','player'])
    dataframe['team_x'].fillna('',inplace=True)
    dataframe['team_y'].fillna('',inplace=True)
    dataframe['team'] = dataframe['team_y'] + dataframe['team_x']
    dataframe.drop(columns=['team_x','team_y'],inplace=True)
    return dataframe
#-----------------------------------------------------------------------------#     
#adding 'home'/'away' to df_pbp dataframe
#getting the 'away'/'home' status of the game in order to merge it with the df_pbp dataframe
#the reason this couldn't be merged above is because we had to include player, and 'Team' wasn't a variable in the df_box df
def add_home_away(dataframe, dataframe2):
    away_home_df = dataframe2.groupby(['link','team'])['team_key'].unique().reset_index()
    away_home_df['team_key'] = away_home_df['team_key'].apply(lambda x: "".join(x))
    dataframe = pd.merge(dataframe, away_home_df, how='left',on=['link','team'])
    return dataframe
#-----------------------------------------------------------------------------#
def add_starters(dataframe, dataframe2):
    starters = dataframe2[dataframe2['role']=='starter'].copy()
    starters = starters.groupby(['link','team'])['player'].unique().reset_index().rename(columns={'player':'starters'})
    
    dataframe = pd.merge(dataframe, starters, on=['link','team'])
    dataframe['changed'] = dataframe['team'].ne(dataframe['team'].shift()).copy()
   
    changed_indeces = dataframe.loc[dataframe['changed']==False].index.tolist()
    dataframe.loc[changed_indeces, 'starters'] = np.nan
    
    dataframe.drop(columns=['changed'],inplace=True)
    dataframe = pd.concat([dataframe, dataframe['starters'].dropna().apply(lambda x: 
                           x.tolist()).apply(lambda x: ','.join(x)).str.split(',', expand=True)],axis=1,join_axes=[dataframe.index])
    return dataframe
#-----------------------------------------------------------------------------#
#dataframe = df.copy()
def more_errors(dataframe):
    if link == '201711290DAL':
        dataframe.loc[636, ['team']] = 'Dallas Mavericks'
        dataframe.loc[642, ['team']] = 'Dallas Mavericks'
    if link == '201711110UTA':
        dataframe.loc[589, ['team']] = 'Brooklyn Nets'
        dataframe.loc[591, ['team']] = 'Brooklyn Nets'
    if link == '201701190MIA':
        dataframe.loc[547, ['team']] = 'Dallas Mavericks'
        dataframe.loc[553, ['team']] = 'Dallas Mavericks'      
    if link =='201712230IND':
        dataframe = dataframe[dataframe['play_index'] != 490]
        dataframe = dataframe[dataframe['play_index'] != 489]
        dataframe = dataframe[dataframe['play_index'] != 493]
        dataframe = dataframe[dataframe['play_index'] != 488]
    if link =='201701160DEN':
        dataframe.loc[463, ['team']] = 'Orlando Magic'
        dataframe.loc[472, ['team']] = 'Orlando Magic'       
    return dataframe
#-----------------------------------------------------------------------------#
#filling in who is in the game and when
#-----------------------------------------------------------------------------#
#test = df.copy()
#dataframe = test.copy()
#column = '2'
#period = '3rd quarter'
#team_key = 'home'
def fill_it_up(dataframe):
    columns = ['0','1','2','3','4']
    dataframe[columns] = None
#-----------------------------------------------------------------------------#
    periods = dataframe['period'].unique().tolist()
    df_period = pd.DataFrame()
    for period in periods:
        df_1 = dataframe[dataframe['period']==period].copy()
        df_team_key = pd.DataFrame()
        for team_key in ['home','away']:
            #print(team_key)
            df = df_1[df_1['team_key']==team_key].copy()
            df.reset_index(drop=True,inplace=True)
#-----------------------------------------------------------------------------#    
        #get all the indeces where player substitutions occur
            indeces = df[df['enters_game'].notnull()].index.tolist()
            del indeces[1::2]#get rid of the hits that happen twice
            
            #if the substituted player doesn't exist in any of the other columns, put it into the selected column
            for column in columns:
                if len(indeces) > 0:
                    if pd.isna(df.loc[indeces[0], column]): #is the column before selected index empty?
                        #is the player that exits at that index in any of the other columns?
                        if df.loc[indeces[0], 'exits_game'] != df.loc[indeces[0], ['0','1','2','3','4']].any():
                            df.loc[indeces[0], column] = df.loc[indeces[0], 'exits_game']
                            df[column].fillna(method='bfill',inplace=True)
                            df.loc[indeces[0] + 1, column] = df.loc[indeces[0] + 1, 'enters_game']
                            
                        #does the player exit the game in that quarter?
                        exit_indeces = indeces.copy()
                        while df.loc[exit_indeces[0]:,'exits_game'].isin([df.loc[exit_indeces[0] + 1, column]]).any():
                            #get the index of where the player is substituted out of the game
                            exit_df = df.loc[exit_indeces[0]:,'exits_game']
                            exit_index = exit_df[exit_df == df.loc[exit_indeces[0] + 1, column]].index.tolist()

                            #fill in the corresponding index where the player is subtituted out of the game
                            df.loc[exit_index[0], column] = df.loc[exit_index[0], 'exits_game']
                            df.loc[exit_index[1], column] = df.loc[exit_index[1], 'enters_game']  
                        
                            #filter the numbers after the exit index out of the for loop 
                            exit_last_index = exit_indeces.index(exit_index[0])
                            indeces = [x for x in indeces if x != exit_index[0]]
                            #exit_indeces = indeces.copy()
                            exit_indeces = exit_indeces[exit_last_index:].copy()
                            if len(exit_indeces) < 1:
                                df[column].fillna(method='ffill',inplace=True)
                                break
                            #if df.loc[exit_indeces[0]:,'exits_game'].isin([df.loc[exit_indeces[0] + 1, column]]).any():
                            #    break
                              #  df[column].fillna(method='ffill',inplace=True)
                               # break                        
                        df[column].fillna(method='ffill',inplace=True)
                        if df[column].notnull().all(): #if the column is full, move to the next index
                            indeces = indeces[1:].copy() #filter out a number from the index
                            #print(column)
                            #print(indeces)
            #check if any of the columns are empty (didn't have any subsititutions)
           
            if df[columns].isnull().any().any():
                empty_columns = []
                for column in columns:
                    if df[column].isnull().any():
                        empty_columns.append(column)

                        
                for column in empty_columns: #iterate through empty columns
                    for i in range(df.index[1],df.index[-1]):
                        if df.loc[i, 'player'] != 'Team': #make sure team isn't included
                            if ~df.loc[i + 1, ['0','1','2','3','4']].isin([df.loc[i, 'player']]).any() and ~df.loc[i - 1, ['0','1','2','3','4']].isin([df.loc[i, 'player']]).any(): #
                                df.loc[i, column] = df.loc[i, 'player']
                                df[column].fillna(method='ffill',inplace=True)
                                df[column].fillna(method='bfill',inplace=True)
                                break
                            else:
                                continue
                        else:
                            continue         
            df_team_key = df_team_key.append(df)
        df_period = df_period.append(df_team_key)
    return df_period
#-----------------------------------------------------------------------------#
def last_step(dataframe):
    dataframe['starters'] = dataframe['0'] + ', ' + dataframe['1'] + ', ' + dataframe['2'] + ', ' + \
                            dataframe['3'] + ', ' + dataframe['4']
    dataframe.sort_index(inplace=True)
    #Combine links dataframe with main dataframe
    dataframe = pd.merge(dataframe,links,how='left',on='link')
    return dataframe
#-----------------------------------------------------------------------------#
def change_time(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    #convert time to datetime format
    dataframe['time'] = ['0' + x if len(x) < 7 else x for x in dataframe['time']]
    dataframe['time'] = [x + '000' for x in dataframe['time']]
    dataframe['time'] = pd.to_datetime(dataframe['time'], format="%M:%S.%f")
    return dataframe
#-----------------------------------------------------------------------------#
def add_playoffs(dataframe):
    dataframe = dataframe.merge(playoffs[['season','playoff_date']], how='left', on='season')
    dataframe['time_of_year'] = np.where(dataframe['playoff_date'] >= dataframe['date'], 'regular_season', 'playoffs')
    dataframe.drop(columns=['playoff_date'],inplace=True)
    return dataframe
#-----------------------------------------------------------------------------#
def add_possessions(dataframe):
    def add_start_end(dataframe):  
        home = home_team
        away = away_team
        
        rank_1 = ['assist', 'block', 'foul','turnover']
        dataframe['possession_rank'] = np.where(dataframe['play'].str.contains('|'.join(rank_1)), 1, 2)
        dataframe.sort_values(by=['play_index','possession_rank'], ascending=[True,True], inplace=True)
        dataframe = dataframe.reset_index(drop=True)
        
        ending_plays = ['turnover','makes 2-pt','makes 3-pt']
        starting_plays = ['defensive rebound']
        end_play_details = ['2 of 2', '3 of 3']
    #-----------------------------------------------------------------------------#
        #who won first possession
        dataframe['home_possession'] = \
        np.where((dataframe['play'].str.contains('|'.join(starting_plays)) & (dataframe['team'] == home)), 'start', 
        np.where((dataframe['play_details'].str.contains('|'.join(end_play_details)) & (dataframe['team'] == home) & (dataframe['play'].str.contains('makes'))), 'end',
        np.where((dataframe['play'].str.contains('|'.join(ending_plays)) & (dataframe['team'] == home)), 'end', None)))
        
        dataframe['away_possession'] = \
        np.where((dataframe['play'].str.contains('|'.join(starting_plays)) & (dataframe['team'] == away)), 'start', 
        np.where((dataframe['play_details'].str.contains('|'.join(end_play_details)) & (dataframe['team'] == away) & (dataframe['play'].str.contains('makes'))), 'end',
        np.where((dataframe['play'].str.contains('|'.join(ending_plays)) & (dataframe['team'] == away)), 'end', None)))

    #-----------------------------------------------------------------------------#    
        if possession == 'away':
            dataframe.loc[0, 'home_possession'] = 'start'
        else:
            dataframe.loc[0, 'away_possession'] = 'start'
            
        dataframe['home_possession'] = np.where(dataframe['away_possession'] == 'end', 'start',
                                         np.where((dataframe['away_possession'] == 'start'), 'end', 
                                               dataframe['home_possession']))
               
        dataframe['away_possession'] = np.where(dataframe['home_possession'] == 'end', 'start',
                                         np.where((dataframe['home_possession'] == 'start'), 'end', 
                                               dataframe['away_possession']))
        
        index = dataframe[dataframe['play_details'].str.contains('1 of 1')==True].index.values
        dataframe.loc[index, ['home_possession','away_possession']] = 'end'
        
        #remove possession ending when there's an 'and one' made shot since player still has to shoot a free throw.
        
        shots_fouls_next_bools = dataframe.loc[dataframe[dataframe['play'].str.contains('makes 2-pt|makes 3-pt')].index.values + 1]['play'].str.contains('foul')
        shots_fouls_next_bools = shots_fouls_next_bools[shots_fouls_next_bools==True].index

        #making sure the fouls happened at the same time as the shot to ensure they're and-1s
        data = dataframe.loc[shots_fouls_next_bools, 'time'].tolist()
        time_to_compare1 = pd.Series(data = data)
        
        data = dataframe.loc[shots_fouls_next_bools - 1, 'time'].tolist()
        time_to_compare2 = pd.Series(data = data)
        
        same_time_index = time_to_compare1 == time_to_compare2
        same_time_index = same_time_index[same_time_index == True].index
        
        shots_with_fouls = dataframe.loc[shots_fouls_next_bools - 1, 'time'].iloc[same_time_index].index
        
        dataframe.loc[shots_with_fouls, ['home_possession', 'away_possession']] = None
        
        return dataframe
    #-----------------------------------------------------------------------------#
    def add_play_number_time(dataframe):
        possession = 'home' if dataframe.loc[0, 'home_possession'] == 'start' else 'away'
        for period in dataframe['period'].unique():
            #quarter end
            quarter_end = dataframe[period == dataframe['period']].index[-1] + .1
            columns = dataframe.columns
            
            df_to_append = pd.DataFrame(columns=columns, index = [quarter_end])
            df_to_append.loc[quarter_end] = dataframe[period == dataframe['period']].iloc[-1]
            
            df_to_append.loc[quarter_end, 'time'] = pd.to_datetime('1900-01-01 00:00:00.0000')
            df_to_append.loc[quarter_end, ['player','distance','play_details','enters_game','exits_game']] = None
            df_to_append.loc[quarter_end, ['play_raw','play']] = 'end_of_period'
            df_to_append.loc[quarter_end, ['home_possession', 'away_possession']] = 'end'
                         
            quarter_start = dataframe[period == dataframe['period']].index[0] - .1
            if ('1st quarter' not in period) and ('overtime' not in period): #filter out first quarter      
                df_to_append.loc[quarter_start] = dataframe.iloc[dataframe[period == dataframe['period']].index[0] - 1]
                df_to_append.loc[quarter_start, 'period'] = period
                df_to_append.loc[quarter_start, 'time'] = pd.to_datetime('1900-01-01 00:12:00.0000')
                df_to_append.loc[quarter_start, ['player','distance','play_details','enters_game','exits_game']] = None
                df_to_append.loc[quarter_start, ['play_raw','play']] = 'start of period'   
                df_to_append.loc[quarter_start, ['home_possession', 'away_possession']] = None
                df_to_append.loc[quarter_start, ['team','team_key','starters','0','1','2','3','4']] = None
                
            #DETERMINE STARTING POSSESSION: alternate the quarters depending on who won possession.        
            if '1st quarter' in period or '4th quarter' in period:
                if possession == 'home': #if the hometeam won possession, they will be 'start'
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                    df_to_append.loc[quarter_start, 'away_possession'] = None
                else: #if away team won possession
                    df_to_append.loc[quarter_start, 'home_possession'] = None
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
                    
            if '2nd quarter' in period or '3rd quarter' in period:
                if possession == 'home': #if the hometeam won possession, away quarter end will be 'start'
                    df_to_append.loc[quarter_start, 'home_possession'] = None
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
                else: #if away team won possession
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                    df_to_append.loc[quarter_start, 'away_possession'] = None
                    
            #OVERTIME#       
            if ('overtime' in period):
                df_to_append.dropna(subset=['period'],inplace=True)
                quarter_start = dataframe[period == dataframe['period']].index[0] - .1
                df_to_append.loc[quarter_start] = dataframe.iloc[dataframe[period == dataframe['period']].index[0] - 1]
                df_to_append.loc[quarter_start, 'period'] = period
                df_to_append.loc[quarter_start, 'time'] = pd.to_datetime('1900-01-01 00:05:00.0000')
                df_to_append.loc[quarter_start, ['player','distance','play_details','enters_game','exits_game']] = None
                df_to_append.loc[quarter_start, ['play_raw','play']] = 'start of period'  
                df_to_append.loc[quarter_start, ['team','team_key','starters','0','1','2','3','4']] = None
                        
           # if ('overtime' in period) and (dataframe['play'][int(quarter_start):int(quarter_start) + 2].str.contains('jump ball|gains').any()):
           #     
           #     starting_index = dataframe[dataframe['period']==period].index[0]
           #     
           #     df_to_append.loc[quarter_start, 'home_possession'] = \
           #     np.where((dataframe['play_raw'].iloc[starting_index].split('(')[1].split(' gains')[0] in dataframe[dataframe['period']==period]['starters'].iloc[0]),
           #              'start', None)
           #     
           #     df_to_append.loc[quarter_start, 'away_possession'] = \
           #     np.where((dataframe['play_raw'].iloc[starting_index].split('(')[1].split(' gains')[0] in dataframe[dataframe['period']==period]['starters'].iloc[1]), 
           #              'start', None) 

            if ('1st overtime' in period):
                if ot1_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
            if ('2nd overtime' in period):
                if ot2_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
            if ('3rd overtime' in period):
                if ot3_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
            if ('4th overtime' in period):
                if ot4_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
            if ('5th overtime' in period):
                if ot5_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'
            if ('6th overtime' in period):
                if ot6_possession == 'home':
                    df_to_append.loc[quarter_start, 'home_possession'] = 'start'
                else:
                    df_to_append.loc[quarter_start, 'away_possession'] = 'start'              
                        
            dataframe = dataframe.append(df_to_append).sort_index().reset_index(drop=True)
            dataframe.dropna(subset=['period'],inplace=True)
        return dataframe        
#-----------------------------------------------------------------------------#    
    def columns_possession_number(dataframe, *columns):
        for column in columns:
            dataframe['possession_number'] = np.where(dataframe[column] == 'start', 1, None)
            
            for i in range(0,dataframe[dataframe['possession_number']==1]['possession_number'].count()):
                location = dataframe[dataframe['possession_number']==1]['possession_number'].index[i]
                dataframe.loc[location, 'possession_number2'] = dataframe.loc[0:location]['possession_number'].sum()
            
            dataframe[column] = np.where(dataframe['possession_number2'].notnull(), dataframe['possession_number2'], dataframe[column])
            dataframe[column].fillna(method='ffill',inplace=True)
            
            dataframe[column] = np.where(dataframe[column]=='end', dataframe[column].shift(1), dataframe[column])
            dataframe[column] = np.where(dataframe[column] =='end', None, dataframe[column])
            dataframe.drop(columns=['possession_number','possession_number2'], inplace=True)

        return dataframe[column]
#-----------------------------------------------------------------------------#    
    def possession_time(dataframe, *columns):
        for column in columns:
            for i in range(1, np.nanmax([x for x in dataframe[column] if type(x) == float]).astype(int) + 1):
                index_start = dataframe[dataframe[column]==i].index[0]
                index_end = dataframe[dataframe[column]==i].index[-1]
                dataframe.loc[index_start:index_end, column + '_time'] = dataframe.loc[index_start, 'time'] - dataframe.loc[index_end, 'time']
        return dataframe[column]        
#-----------------------------------------------------------------------------#        
    def final(dataframe):
        dataframe = add_start_end(dataframe)
        dataframe = add_play_number_time(dataframe)
        columns_possession_number(dataframe, 'home_possession', 'away_possession')
        possession_time(dataframe, 'home_possession', 'away_possession')
        return dataframe            

    dataframe = final(dataframe)
    
    return dataframe
#-----------------------------------------------------------------------------#
value = '201511110CHO'
def bulk_main(df_unique, season):
    df_all = pd.DataFrame()  
    pbar = tqdm([season])
    for char in pbar:
        pbar.set_description("Processing %s" % char)
    for value in tqdm(df_unique['link']):
        try:
            global link #creating a global link in order to use it in nested functions
            link = value
            df = df_pbp_raw[df_pbp_raw['link'].isin([link])]
            df = df.reset_index(drop=True)
            #set up some global variables
            global away_team
            away_team = df['home_away'][0].split(',')[0]
            global home_team
            home_team = df['home_away'][0].split(', ')[1]
            date = df['date'][0]
            df = pd.DataFrame(df,columns=['text']).reset_index(drop=True)
            df[df['text'].str.contains('timeout',na=False)] = df[df['text'].str.contains('timeout',na=False)].apply(lambda x: x + ' by Team')
            #-----------------------------------------------------------------------------#
            df = add_blank_rows(df, 'text', string='Jump')
            #-----------------------------------------------------------------------------#
            strings_to_remove = ['/players/' + x + '/' for x in (list(string.ascii_lowercase))]
            strings_to_remove.extend(['colspan="5">','class=','center"','"','<td','td>','<','>','=','bbr-play-score',
                                      'a href','/a','bbr-play-tie','bbr-play-leadchange', '.html','/'])
            strings_to_remove = tuple(strings_to_remove)
        
            #strings_to_remove.extend(players)

            remove_strings(df, 'text', *strings_to_remove)
            #-----------------------------------------------------------------------------#
            global possession
            possession = get_possession(df)
            global ot1_possession
            ot1_possession = get_overtime_possessions(df, '1st overtime')
            global ot2_possession
            ot2_possession = get_overtime_possessions(df, '2nd overtime')
            global ot3_possession
            ot3_possession = get_overtime_possessions(df, '3rd overtime')
            global ot4_possession
            ot4_possession = get_overtime_possessions(df, '4th overtime')
            global ot5_possession
            ot5_possession = get_overtime_possessions(df, '5th overtime')
            global ot6_possession
            ot6_possession = get_overtime_possessions(df, '6th overtime')
            #-----------------------------------------------------------------------------#
            df = fix_errors(df, link)
            #-----------------------------------------------------------------------------#
            df = add_quarter(df, 'text', 'period', 
                             '2nd quarter','3rd quarter','4th quarter','1st overtime','2nd overtime',
                             '3rd overtime','4th overtime','5th overtime','6th overtime')
            #-----------------------------------------------------------------------------#
            df = add_team_subs(df, home_team, away_team)
            #-----------------------------------------------------------------------------#
            df = pull_data(df)
            #-----------------------------------------------------------------------------#
            df = fix_score(df)
            #-----------------------------------------------------------------------------#
            df = final_fixes(df, date, link)
            #-----------------------------------------------------------------------------#
            box = get_starters(df_box, link)
            #-----------------------------------------------------------------------------#
            df = merge_dfs(df, box)
            #-----------------------------------------------------------------------------#
            df = more_errors(df)
            df = add_home_away(df, box)
            #-----------------------------------------------------------------------------#
            df = add_starters(df, box)
            #-----------------------------------------------------------------------------#
            df.columns = df.columns.astype(str)
            df = fill_it_up(df) 
            #-----------------------------------------------------------------------------#
            df = last_step(df)
            #-----------------------------------------------------------------------------#
            change_time(df)
            #-----------------------------------------------------------------------------#
            df = add_playoffs(df)
            #-----------------------------------------------------------------------------#
            df = add_possessions(df)
            #-----------------------------------------------------------------------------#
            df_all = df_all.append(df)
            #-----------------------------------------------------------------------------#
        except:
            logging.exception(str(link))
            print(str(link))
            continue
    return df_all
#-----------------------------------------------------------------------------#
def main(*seasons):
    for season in seasons:
        filtered_df = df_pbp_raw[df_pbp_raw['season'] == season]
        df_unique = get_unique(filtered_df)
        df_all = bulk_main(df_unique, season)
        df_all.to_csv('nba_analysis/df_complete_pbp_' + str(season) + '.csv', index=False)
#-----------------------------------------------------------------------------#
main(2012, 2013, 2014)
#-----------------------------------------------------------------------------#

#import pandas as pd
#import numpy as np
#import completed dataframe
#df_all = pd.read_csv('nba_analysis/df_complete_pbp_2018.csv')
#df_all = df_all.sort_values(by=['link','team_key'])

#check if same player is in more than one column at a time
#def test(dataframe):
#    problem_rows = []
#    dataframe.dropna(subset=['starters'],inplace=True)
#    for i in tqdm(range(len(dataframe))):
#        if dataframe.iloc[i][['0','1','2','3','4']].nunique() < 5:
#            problem_rows.append(i)
#        else:
#            continue
#    if problem_rows == 0:
#        return 'all good!'
#    else:
#        return problem_rows
 





#-----------------------------------------------------------------------------#
#problem = test(df_all)
#problem2 = test(check)

#check = df_all.loc[problem]
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
#test = df_all[df_all['play_details'].str.contains('3 sec', na=False)]

#df_all.drop(columns=['enters_game', 'exits_game', 'score_home','score_away','score_diff','date',
#       'play_index','starters', '0', '1', '2', '3', '4', 'season', 'key', 'time_of_year','possession_rank'],inplace=True)


#test = df_all[df_all['link']=='201803310MIA']

#df_all['link'].nunique()

#jump = df_all[df_all['play'].str.contains('jump')]

#test1 = df_pbp_raw[df_pbp_raw['season']==2018]
#test2 = test1[test1['text'].str.contains('Violation')]

#test = df_all[df_all['play'].str.contains('violation', na=False)]



#df.sort_values(by=['link','play_index'], inplace=True)