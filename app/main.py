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

def getMatchesDataCsv(account_id, query_params={}, api_calls_limit=60, api_limit_duration_seconds=60):
    api_calls_made = 0
    rows = []
    match_fields = ['match_id', 'duration', 'first_blood_time', 'dire_score', 'radiant_score', 'radiant_win', 'start_time', 'region', 'replay_url', 'picks_bans']
    player_fields = ['player_slot', 'ability_upgrades_arr', 'assists', 'backpack_0', 'backpack_1', 'backpack_2', 'deaths', 'denies', 'gold', 'gold_spent', 'hero_damage', 'hero_healing', 'hero_id', 'item_0', 'item_1', 'item_2', 'item_3', 'item_4', 'item_5', 'item_neutral', 'kills', 'last_hits', 'level', 'net_worth', 'permanent_buffs', 'tower_damage', 'isRadiant', 'total_gold', 'total_xp', 'kda', 'rank_tier']
    benchmark_fields = ['gold_per_min', 'xp_per_min', 'kills_per_min', 'last_hits_per_min', 'hero_damage_per_min', 'hero_healing_per_min', 'tower_damage']
    filename = f"player-{account_id}-matches.csv"
    matches_data = requests.get(f'https://api.opendota.com/api/players/{account_id}/matches', params=query_params)
    api_calls_made += 1
    matches_data_json = json.loads(matches_data.text)

    for match in matches_data_json:
        if api_calls_made < api_calls_limit:
            row = []
            match_id = match['match_id']
            match_data_detailed = requests.get(f'https://api.opendota.com/api/matches/{match_id}')
            api_calls_made += 1
            data = json.loads(match_data_detailed.text)
            row = addToRow(match_fields, data, row)
            for player in data['players']:
                if player['account_id'] == account_id:
                    row = addToRow(player_fields, player, row)
                    row = addToRow(benchmark_fields, player['benchmarks'], row)
            rows.append(row)
        else:
            print(f'waiting for {api_limit_duration_seconds} seconds before making next set of calls......')
            time.sleep(api_limit_duration_seconds)
            api_calls_made = 0
    
    # Write to csv
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(match_fields + player_fields + benchmark_fields)
        csvwriter.writerows(rows)

def main():
    # params - limit, game_mode, hero_id, win, is_radiant, with_hero_id[], against_hero_id[], included_account_id[]
    getMatchesDataCsv(108755293)

if __name__ == "__main__":
    main()