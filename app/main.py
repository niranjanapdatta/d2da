import logging
import time
import requests
import json
import time
import csv


def addToRow(fields, data, row=[]):
    for field in fields:
        try:
            row.append(data[field])
        except KeyError:
            row.append(None)
    return row

def dictKeyToSortBy(item):
  return item['order']

def getMatchesDataCsv(account_id, query_params={}, api_calls_limit=55, api_limit_duration_seconds=60):
    count = 0
    api_calls_made = 0
    rows = []
    match_fields = ['match_id', 'duration', 'first_blood_time', 'game_mode', 'dire_score', 'radiant_score', 'radiant_win', 'start_time', 'region', 'patch', 'throw', 'comeback', 'replay_url']
    player_fields = ['player_slot', 'ability_upgrades_arr', 'assists', 'backpack_0', 'backpack_1', 'backpack_2', 'deaths', 'denies', 'gold', 'gold_spent', 'hero_damage', 'hero_healing', 'hero_id', 'item_0', 'item_1', 'item_2', 'item_3', 'item_4', 'item_5', 'item_neutral', 'kills', 'last_hits', 'level', 'net_worth', 'permanent_buffs', 'tower_damage', 'isRadiant', 'total_gold', 'total_xp', 'kda', 'rank_tier']
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

def main():
    logging.basicConfig(filename=f'main-log-{str(time.time())}.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)  
    getMatchesDataCsv(108755293)

if __name__ == "__main__":
    main()