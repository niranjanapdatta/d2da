import logging
import time
import json
import time
import csv
import operator

# External Libraries
import requests
import pandas as pd
import seaborn as sns


def printnl(string):
    print(f'{string} \n')


def printTupleItems(tuple):
    for item in tuple:
        printnl(item)


def round2(num):
    return round(num, 2)


def addToRow(fields, data, row=[]):
    for field in fields:
        try:
            row.append(data[field])
        except KeyError:
            row.append(None)
    return row


def dictKeyToSortBy(item):
  return item['order']


def getMeanAggregate(df, key, keyString):
    hero_data = requests.get('https://api.opendota.com/api/heroes')
    hero_data_json = json.loads(hero_data.text)
    mean = df.groupby('hero_id')[key].mean()
    max_mean = mean.max()
    max_mean_hero_id = mean.idxmax()
    min_mean = mean.min()
    min_mean_hero_id = mean.idxmin()
    max_hero = ''
    min_hero = ''
    for hero in hero_data_json:
        if hero['id'] == max_mean_hero_id:
            max_hero = hero['localized_name']
        if hero['id'] == min_mean_hero_id:
            min_hero = hero['localized_name']
    return f'Highest average {keyString}= {round2(max_mean)} (Hero: {max_hero})',\
        f'Lowest average {keyString}= {round2(min_mean)} (Hero: {min_hero})'


def splitListOfHeroes(df):
    df['list_of_picks_formatted_radiant'] = None
    df['list_of_picks_formatted_dire'] = None
    for index, row in df.iterrows():
        try:
            list_of_picks = json.loads(str(row['list_of_picks']).replace("'", '"'))
            list_picks_formatted_radiant = []
            list_picks_formatted_dire = []
            if len(list_of_picks) == 10:
                for i in range(10):
                    if(list_of_picks[i]['team'] == 0):
                        list_picks_formatted_radiant.append(list_of_picks[i]['hero_id'])
                    else:
                        list_picks_formatted_dire.append(list_of_picks[i]['hero_id'])
            df.at[index, 'list_of_picks_formatted_radiant'] = list_picks_formatted_radiant
            df.at[index, 'list_of_picks_formatted_dire'] = list_picks_formatted_dire
        except json.decoder.JSONDecodeError:
            continue
    return df


def populatePlayerMatchesDataInCsv(account_id, query_params={}, api_calls_limit=55, api_limit_duration_seconds=60):
    count = 0
    api_calls_made = 0
    rows = []
    match_fields = ['match_id', 'duration', 'first_blood_time', 'game_mode', 'dire_score', 'radiant_score', 'radiant_win', 'start_time', 'region', 'patch', 'throw', 'comeback', 'replay_url']
    player_fields = ['player_slot', 'ability_upgrades_arr', 'assists', 'backpack_0', 'backpack_1', 'backpack_2', 'deaths', 'denies', 'gold', 'gold_spent', 'hero_damage', 'hero_healing', 'hero_id', 'item_0', 'item_1', 'item_2', 'item_3', 'item_4', 'item_5', 'item_neutral', 'kills', 'last_hits', 'level', 'net_worth', 'permanent_buffs', 'isRadiant', 'total_gold', 'total_xp', 'kda', 'rank_tier']
    benchmark_fields = ['gold_per_min', 'xp_per_min', 'kills_per_min', 'last_hits_per_min', 'hero_damage_per_min', 'hero_healing_per_min', 'tower_damage']
    other_fields = ['list_of_picks']
    filename = f"player-{account_id}-matches.csv"
    matches_data = requests.get(f'https://api.opendota.com/api/players/{account_id}/matches', params=query_params)
    api_calls_made += 1
    matches_data_json = json.loads(matches_data.text)

    for match in matches_data_json:
        if api_calls_made < api_calls_limit:
            row = []
            match_id = None
            try:
                match_id = match['match_id']
            except KeyError:
                logging.info('Could not get match_id')
                continue
            match_data_detailed = requests.get(f'https://api.opendota.com/api/matches/{match_id}')
            api_calls_made += 1
            data = json.loads(match_data_detailed.text)
            row = addToRow(match_fields, data, row)
            picks_hero_ids = []
            picks_hero_map = {}
            try:
                for pick_ban in data['picks_bans']:
                    if pick_ban['is_pick']:
                        picks_hero_ids.append(pick_ban['hero_id'])
                        picks_hero_map.update({pick_ban['hero_id']: {'hero_id': pick_ban['hero_id'], 'team': pick_ban['team'], 'order': pick_ban['order']}})
            except KeyError:
                logging.error(f'KeyError occured while getting picks_bans data for match with id {match_id}')
            except TypeError:
                logging.error(f'TypeError occured while getting picks_bans data for match with id {match_id}')
            list_of_picks = []
            try:
                for player in data['players']:
                    if player['hero_id'] in picks_hero_ids:
                        list_of_picks.append(picks_hero_map.get(player['hero_id']))
                    if player['account_id'] == account_id:
                        row = addToRow(player_fields, player, row)
                        row = addToRow(benchmark_fields, player['benchmarks'], row)
                list_of_picks.sort(key=dictKeyToSortBy)
            except KeyError:
                logging.error(f'KeyError occured while getting data of player with account id {account_id} for match with id {match_id}')
            except TypeError:
                logging.error(f'TypeError occured while getting picks_bans data for match with id {match_id}')
            row.append(list_of_picks)
            rows.append(row)
            print(f'fetched data for match with id: {match_id}')
            logging.info(f'fetched data for match with id: {match_id}')
        else:
            print(f'waiting for {api_limit_duration_seconds} seconds before making next set of calls......')
            logging.info(f'waiting for {api_limit_duration_seconds} seconds before making next set of calls......')
            time.sleep(api_limit_duration_seconds)
            api_calls_made = 0
        count += 1
        print(f'{count} matches data have been fetched')
        logging.info(f'{count} matches data have been fetched')
    
    # Write to csv
    with open(filename, 'w', newline='') as csvfile:
        logging.info('writing data to .csv file...')
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(match_fields + player_fields + benchmark_fields + other_fields)
        csvwriter.writerows(rows)
        logging.info('data has been written to .csv file')


def getDf(path_to_player_matches_data_csv):
    print(f'reading data from .csv file. Path: {path_to_player_matches_data_csv}')
    try:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(path_to_player_matches_data_csv)
        df = df[df.hero_id != 0]

        print(f'read {len(df.index)} records from .csv file')
        print('\n')
        print(f"Player (Account ID: {path_to_player_matches_data_csv.split('-')[1]}) Analysis: ")
        print()

        # Additional Columns
        df['gpm'] = df['total_gold'] / (df['duration'] / 60)
        df['xpm'] = df['total_xp'] / (df['duration'] / 60)
        df['win'] = ((df['isRadiant']) & (df['radiant_win'])) | ((df['isRadiant'] == False) & (df['radiant_win'] == False))

        return df

    except FileNotFoundError:
        printnl(f'could not find the .csv file at path: {path_to_player_matches_data_csv}')
        return None


def getOverallPlayerAnalysis(path_to_player_matches_data_csv):
    df = getDf(path_to_player_matches_data_csv)
    if df is not None and not df.empty:
        win = df['win']
        loss = (df['win'] == False)

        # Total wins and losses
        total_wins = df.where(win) \
            .groupby('isRadiant')['radiant_win'] \
            .count() \
            .sum()
        total_losses = df.where(loss) \
            .groupby('isRadiant')['radiant_win'] \
            .count() \
            .sum()
        total_matches = total_wins + total_losses
        printnl(f'----General Stats----')
        printnl(f'Total matches played= {total_matches}')
        printnl(f'Total wins= {total_wins}')
        printnl(f'Total losses= {total_losses}')

        win_percentage = round2((total_wins / total_matches) * 100)
        loss_percentage = round2((total_losses / total_matches) * 100)
        printnl(f'Win percentage= {win_percentage}')
        printnl(f'Loss percentage= {loss_percentage}')
        print()

        printnl(f'----Hero Based Stats----')
        heroes_total_wins = df.where(win).groupby('hero_id')['hero_id'].count()
        heroes_total_wins = pd.DataFrame({'hero_id':heroes_total_wins.index, 'wins':heroes_total_wins.values})
        heroes_total_losses = df.where(loss).groupby('hero_id')['hero_id'].count()
        heroes_total_losses = pd.DataFrame({'hero_id':heroes_total_losses.index, 'losses':heroes_total_losses.values})
        heroes_matches_wins_losses = pd.merge(heroes_total_wins, heroes_total_losses, how="outer")

        # Highest and Lowest Averages
        printTupleItems(getMeanAggregate(df, 'gpm', 'Gold Per Minute'))
        printTupleItems(getMeanAggregate(df, 'xpm', 'Experience Per Minute'))
        printTupleItems(getMeanAggregate(df, 'kills', 'Kills'))
        printTupleItems(getMeanAggregate(df, 'deaths', 'Deaths'))
        printTupleItems(getMeanAggregate(df, 'assists', 'Assists'))
        printTupleItems(getMeanAggregate(df, 'net_worth', 'Networth'))
        printTupleItems(getMeanAggregate(df, 'hero_damage', 'Hero Damage'))
        printTupleItems(getMeanAggregate(df, 'kda', 'KDA (Kills-Deaths-Assists)'))
        printTupleItems(getMeanAggregate(df, 'last_hits', 'Last Hits'))
        printTupleItems(getMeanAggregate(df, 'level', 'Level'))


def getHeroPerformanceAgainst(path_to_player_matches_data_csv, hero_id, enemy_hero_id):
    df = getDf(path_to_player_matches_data_csv)
    if df is not None and not df.empty:
        df = splitListOfHeroes(df)
        # list_of_picks_formatted_radiant
        for index, row in df.iterrows():
            if row['list_of_picks_formatted_dire'] and hero_id in row['list_of_picks_formatted_dire']:
                print("YES")
        

def getProTeamMatches(team_id, api_calls_limit=55, api_limit_duration_seconds=60):
    api_calls_made = 0
    filename = f"team-{team_id}-matches.csv"
    matches_data = requests.get(f'https://api.opendota.com/api/teams/{team_id}/matches')
    api_calls_made += 1
    matches_data_json = json.loads(matches_data.text)
    rows = []
    count = 0
    for match in matches_data_json:
        if api_calls_made < api_calls_limit:
            match_data = requests.get(f'https://api.opendota.com/api/matches/{match["match_id"]}')
            match_data_json = json.loads(match_data.text)
            match_data_keys = ['radiant']
            match_fields = ['match_id', 'duration', 'first_blood_time', 'game_mode', 'dire_score', 'radiant_score', 'radiant_win', 'start_time', 'region', 'patch', 'throw', 'comeback', 'replay_url', 'picks_bans']
            row = []
            row = addToRow(match_data_keys, match, row)
            row = addToRow(match_fields, match_data_json, row)
            # Write to csv
            rows.append(row)
            count += 1
            api_calls_made += 1
            print(f'fetched {count} matches data')
        else:
            print('waiting for 60 sec')
            time.sleep(api_limit_duration_seconds)
            api_calls_made = 0
    
    with open(filename, 'w', newline='') as csvfile:
        logging.info('writing data to .csv file...')
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(match_data_keys + match_fields)
        csvwriter.writerows(rows)
        logging.info('data has been written to .csv file')


def draftAssistant(path_to_pro_team_matches_data_csv, enemy_heroes_list):
    print('----DRAFT ASSISSTANT----\n')
    if len(enemy_heroes_list) > 5 or len(enemy_heroes_list) < 1:
        print(f'Please provide 1-5 enemy hero ids')
        return 0, 0, 0, 0
    print(f'reading data from .csv file. Path: {path_to_pro_team_matches_data_csv}')
    team_id = path_to_pro_team_matches_data_csv.split('-')[1]
    try:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(path_to_pro_team_matches_data_csv)

        # Additional Columns
        df['win'] = ((df['radiant']) & (df['radiant_win'])) | ((df['radiant'] == False) & (df['radiant_win'] == False))

        print(f'read {len(df.index)} records from .csv file')
        print('\n')
        print(f"Team (Team ID: {team_id}) Analysis: ")
        print()
        
        df_valid = df.where(df['radiant_win'].notnull())
        df_picks_bans = df_valid.where(df_valid['picks_bans'].notnull())
        wins = 0
        losses = 0
        best_heroes_against = {}
        for index, row in df_picks_bans.iterrows():
            try:
                picks_bans = json.loads(str(row['picks_bans'])
                    .replace("'", '"')
                    .replace('False','"FALSE"')
                    .replace('True', '"TRUE"'))
                picks_bans_df = pd.json_normalize(picks_bans)
                if row['radiant']:
                    _wins, _losses, _best_heroes_against = getDetailsAgainst(enemy_heroes_list, row, picks_bans_df, 1, 0, best_heroes_against)
                    wins += _wins
                    losses += _losses
                    best_heroes_against = _best_heroes_against
                else:
                    _wins, _losses, _best_heroes_against = getDetailsAgainst(enemy_heroes_list, row, picks_bans_df, 0, 1, best_heroes_against)
                    wins += _wins
                    losses += _losses
                    best_heroes_against = _best_heroes_against
            except json.decoder.JSONDecodeError:
                continue

        return wins, losses, best_heroes_against, enemy_heroes_list
        
    except FileNotFoundError:
        printnl(f'could not find the .csv file at path: {path_to_pro_team_matches_data_csv}')


def getHeroesData():
    hero_data = requests.get('https://api.opendota.com/api/heroes')
    hero_data_json = json.loads(hero_data.text)
    df = pd.DataFrame.from_dict(hero_data_json)
    return df


def getDetailsAgainst(enemy_heroes_list, row, picks_bans_df, enemy_team_id, ally_team_id, best_heroes_against):
    wins = 0
    losses = 0
    if picks_bans_df.where((picks_bans_df['team'] == enemy_team_id) & (picks_bans_df['is_pick'] == 'TRUE')) \
        [picks_bans_df['hero_id']
        .isin(enemy_heroes_list)]['hero_id'].count() == len(enemy_heroes_list):
        condition = row['radiant_win'] if enemy_team_id == 1 else not row['radiant_win']
        if condition:
            wins += 1
            for hero_id in picks_bans_df.loc[(picks_bans_df['team'] == ally_team_id) & (picks_bans_df['is_pick'] == 'TRUE')]['hero_id'].values:
                if hero_id in best_heroes_against:
                    best_heroes_against.update({hero_id: best_heroes_against.get(hero_id) + 1})
                else:
                    best_heroes_against.update({hero_id: 1})
        else:
            losses += 1
    return wins, losses, best_heroes_against


def main():
    # Logging Config
    # logging.basicConfig(filename=f'main-log-{str(time.time())}.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)  

    heroes_data = getHeroesData()
    heroes_data.rename(columns = {'id':'hero_id'}, inplace = True)

    account_id = 108755293
    # populatePlayerMatchesDataInCsv(account_id)

    path_player_matches_data = "player-108755293-matches.csv"
    # getOverallPlayerAnalysis(path_player_matches_data)

    team_id = 2586976
    # getProTeamMatches(team_id)

    path_pro_team_matches_data = "team-2586976-matches.csv"
    enemy_heroes_list = [2, 10]
    wins, losses, best_heroes_against, enemy_heroes_list = draftAssistant(path_pro_team_matches_data, enemy_heroes_list)
    try:
        df_enemy_heroes = pd.DataFrame(enemy_heroes_list, columns=['hero_id'])
        df_enemy_heroes_mapped = pd.merge(df_enemy_heroes, heroes_data, on=['hero_id'])
        enemies = df_enemy_heroes_mapped["localized_name"].values
        if (wins + losses) > 0:
            printnl(f'Wins against enemy heroes {enemies}= {wins}')
            printnl(f'Losses against enemy heroes with ids {enemies}= {losses}')
            if len(best_heroes_against) > 0:
                df_best_heroes_against = pd.DataFrame.from_dict(dict(sorted(best_heroes_against.items(), key=operator.itemgetter(1), reverse=True)[:5]), orient='index', columns=['wins'])
                best_heroes_against_mapped = pd.merge(df_best_heroes_against, heroes_data, left_index=True ,right_on=['hero_id'])
                best_heroes_against_mapped_hero_names = best_heroes_against_mapped["localized_name"].values
                printnl(f'Best heroes against the enemy line-up= {best_heroes_against_mapped_hero_names}')
            else:
                printnl(f'Insufficient data to suggest picks')
        else:
            printnl(f'Insufficient data for enemy heroes with ids {enemies}')
    except ValueError:
        printnl(f'No data found')


if __name__ == "__main__":
    main()