import logging
import time
import json
import time
import csv

# External Libraries
import requests
import pandas as pd


def addToRow(fields, data, row=[]):
    for field in fields:
        try:
            row.append(data[field])
        except KeyError:
            row.append(None)
    return row


def dictKeyToSortBy(item):
  return item['order']


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


def getOverallPlayerAnalysis(pathToPlayerMatchesDataCsv):
    print(f'reading data from .csv file. Path: {pathToPlayerMatchesDataCsv}')
    try:
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(pathToPlayerMatchesDataCsv)
        print(f'read {len(df.index)} records from .csv file')
        print('\n')
        print(f"Player (Account ID: {pathToPlayerMatchesDataCsv.split('-')[1]}) Analysis: ")

        # Total wins and losses
        match_win_condition_expression = ((df['isRadiant']) & (df['radiant_win'])) | ((df['isRadiant'] == False) & (df['radiant_win'] == False))
        match_loss_condition_expression = ((df['isRadiant']) & (df['radiant_win'] == False)) | ((df['isRadiant'] == False) & (df['radiant_win']))

        total_wins = df.where(match_win_condition_expression) \
            .groupby('isRadiant')['radiant_win'] \
            .count() \
            .sum()
        total_losses = df.where(match_loss_condition_expression) \
            .groupby('isRadiant')['radiant_win'] \
            .count() \
            .sum()
        total_matches = total_wins + total_losses
        print(f'Total matches played= {total_matches}')
        print(f'Total wins= {total_wins}')
        print(f'Total losses= {total_losses}')

        win_percentage = round(((total_wins / total_matches) * 100), 2)
        loss_percentage = round(((total_losses / total_matches) * 100), 2)
        print(f'Win percentage= {win_percentage}')
        print(f'Loss percentage= {loss_percentage}')

        # All hero stats
        heroes_total_wins = df.where(match_win_condition_expression).groupby('hero_id')['hero_id'].count()
        heroes_total_wins = pd.DataFrame({'hero_id':heroes_total_wins.index, 'wins':heroes_total_wins.values})
        heroes_total_losses = df.where(match_loss_condition_expression).groupby('hero_id')['hero_id'].count()
        heroes_total_losses = pd.DataFrame({'hero_id':heroes_total_losses.index, 'losses':heroes_total_losses.values})
        heroes_matches_wins_losses = pd.merge(heroes_total_wins, heroes_total_losses, how="outer")
        print(heroes_matches_wins_losses)

    except FileNotFoundError:
        print(f'could not find the .csv file at path: {pathToPlayerMatchesDataCsv}')


def main():
    # Logging Config
    # logging.basicConfig(filename=f'main-log-{str(time.time())}.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)  

    # populatePlayerMatchesDataInCsv(108755293)
    getOverallPlayerAnalysis("player-108755293-matches.csv")


if __name__ == "__main__":
    main()