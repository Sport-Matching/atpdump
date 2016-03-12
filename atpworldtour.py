#! /usr/bin/env python3
import json
import glob
import sys
import re
import datetime
from lxml import html
import requests
import psycopg2

baseUrl = 'http://www.atpworldtour.com'


def connect_sql():
    host = "127.0.0.1"
    port = 5432
    db = "sport_matching"
    user = "dev"
    password = "dev"
    conn_string = "host='" + host + "' port='" + str(port) + "' dbname='" + db + \
                  "' user='" + user + "' password='" + password + "'"
    return psycopg2.connect(conn_string)

dbInstance = connect_sql()


def clear_db(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM tournaments")
    cur.execute("DELETE FROM matches")
    cur.execute("DELETE FROM sets")
    cur.execute("DELETE FROM rankings")
    cur.execute("DELETE FROM players")
    cur.close()
    conn.commit()


def clear_matches(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM sets")
    cur.execute("DELETE FROM matches")
    cur.execute("DELETE FROM tournaments")
    cur.close()
    conn.commit()


def may_insert_ground(conn, ground):
    cur = conn.cursor()
    cur.execute("SELECT ground_id FROM sp_may_insert_ground('42');")
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def may_insert_tournament(conn, tournament):
    cur = conn.cursor()
    cur.execute("SELECT tournament_id FROM sp_may_insert_tournament(%s, %s, %s);",
                (tournament['name'], tournament['ground_type'], tournament['year']))
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def insert_player(conn, player):
    cur = conn.cursor()
    cur.execute("insert into players (name, birthdate, sex, country, weight, size, picture_url) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;", (player['name'], player['birthDate'],
                                                                      player['sex'], player['country'],
                                                                      player['weight'], player['size'],
                                                                      player['pictureUrl']))
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def insert_set(conn, new_set):
    cur = conn.cursor()
    cur.execute("insert into sets (player1_score, player2_score, player1_tie_break, player2_tie_break)"
                "VALUES (%s, %s, %s, %s) RETURNING id;",
                (new_set['player1_score'], new_set['player1_score'], new_set['player1_tie_break'],
                 new_set['player2_tie_break']))
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def insert_match(conn, match):
    cur = conn.cursor()
    cur.execute("insert into matches (player1_id, player2_id, set1_id, set2_id, set3_id, set4_id, set5_id, date,"
                "tournament_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;",
                (match['player1_id'], match['player2_id'], match['set1_id'], match['set2_id'], match['set3_id'],
                 match['set4_id'], match['set5_id'], match['date'], match['tournament_id']))
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def get_player_by_url(url, players):
    print(url)
    exit(0)
    for player_name in players:
        player = players[player_name]
        if player['overviewUrl'] == url or player['activityUrl'] == url:
            return player
    return None


def download_player(player_activity_url, player_overview_url, session):
    player_activity_html = session.get(player_activity_url)
    player_activity_tree = html.fromstring(player_activity_html.text)

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-overflow']"
                                   "/div[@class='player-profile-hero-dash']/div[@class='inner-wrap']"
                                   "/div[@class='player-profile-hero-name']")
    if len(e) == 0 or len(e[0]) <= 1:
        return None
    name = e[0][0].text.strip() + " " + e[0][1].text.strip()

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-overflow']"
                                   "/div[@class='player-profile-hero-table']/div[@class='inner-wrap']/table/tr[1]"
                                   "/td[1]/div[@class='wrap']/div[@class='table-big-value']"
                                   "/span[@class='table-birthday-wrapper']/span")
    if len(e) > 0:
        birth_date_string = e[0].text.strip()
    else:
        birth_date_string = "(1970.01.01)"
    birth_date = datetime.datetime.strptime(birth_date_string, "(%Y.%m.%d)")

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-overflow']"
                                   "/div[@class='player-profile-hero-dash']/div[@class='inner-wrap']"
                                   "/div[@class='player-profile-hero-ranking']/div[@class='player-flag']"
                                   "/div[@class='player-flag-code']")
    if len(e) > 0:
        country = e[0].text.strip()
    else:
        country = ""

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-image']/img")
    if len(e) > 0:
        player_picture_url = baseUrl + e[0].attrib['src']
    else:
        player_picture_url = None

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-overflow']"
                                   "/div[@class='player-profile-hero-table']/div[@class='inner-wrap']/table/tr[1]"
                                   "/td[3]/div[@class='wrap']/div[@class='table-big-value']"
                                   "/span[@class='table-weight-kg-wrapper']")
    if len(e) > 0:
        weight = e[0].text.strip()
        m = re.search("\(([0-9]+)kg\)", weight)
        if m is not None:
            weight = int(m.group(1)) * 1000
        else:
            weight = 0
    else:
        weight = 0

    e = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                   "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                   "/div[@class='player-profile-hero-overflow']"
                                   "/div[@class='player-profile-hero-table']/div[@class='inner-wrap']/table/tr[1]"
                                   "/td[4]/div[@class='wrap']/div[@class='table-big-value']"
                                   "/span[@class='table-height-cm-wrapper']")
    if len(e) > 0:
        size = e[0].text.strip()
        m = re.search("\(([0-9]+)cm\)", size)
        if m is not None:
            size = int(m.group(1))
        else:
            size = 0
    else:
        size = 0

    player = {
        'id': None,
        'overviewUrl': player_overview_url,
        'activityUrl': player_activity_url,
        'pictureUrl': player_picture_url,
        'name': name,
        'birthDate': str(birth_date),
        'sex': '0',
        'country': country,
        'weight': weight,
        'size': size
    }
    player['id'] = insert_player(dbInstance, player)
    return player


def download_players(begin, end):

    get_ranking_url = baseUrl + '/en/rankings/singles/?rankDate=2016-2-29&countryCode=all&rankRange=' +\
                    str(begin) + '-' + str(end)
    players = {}

    session = requests.session()
    all_players_html = session.get(get_ranking_url)
    all_players_tree = html.fromstring(all_players_html.text)
    all_players_tree_elements = all_players_tree.xpath("/html/body/div[@id='mainLayoutWrapper']"
                                                       "/div[@id='backbonePlaceholder']/div[@id='mainContainer']"
                                                       "/div[@id='mainContent']/div[@id='singlesRanking']"
                                                       "/div[@id='rankingDetailAjaxContainer']"
                                                       "/table[@class='mega-table']/tbody/tr")

    for playerTreeElement in all_players_tree_elements:
        name_element = playerTreeElement[3][0]
        player_overview_url = baseUrl + name_element.attrib['href']
        player_activity_url = player_overview_url[:-8] + "player-activity?year=all&matchType=singles"
        player = download_player(player_activity_url, player_overview_url, session)
        if player is not None:
            players[player['name']] = player
    with open("out/players-" + str(begin) + "-" + str(end) + ".json", "w+") as f:
        f.write(json.dumps(players))
    exit(0)


def download_matches(begin, end):

    players = {}
    tournaments = {}
    tournament_ground_types = {}
    matches = []
    session = requests.session()
    for filename in glob.glob("out/players-*.json"):
        with open(filename, "r") as f:
            data = json.load(f)
            for playerName in data:
                players[playerName] = data[playerName]
    with open("out/players-" + str(begin) + "-" + str(end) + ".json", "r") as f:
        selected_players = json.load(f)

    for player_name in selected_players:
        player = players[player_name]
        player_activity_html = session.get(player['activityUrl'])
        player_activity_tree = html.fromstring(player_activity_html.text)

        tournaments_tree = player_activity_tree.xpath("/html/body/div[@id='mainLayoutWrapper']"
                                                      "/div[@id='backbonePlaceholder']/div[@id='mainContainer']"
                                                      "/div[@id='mainContent']/div[@id='currentTabContent']/div[3]"
                                                      "/div[@class='activity-tournament-table']")
        for tournamentTree in tournaments_tree:
            e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                     "/tr[@class='tourney-result with-icons']/td[@class='title-content']"
                                     "/*[@class='tourney-title']")
            tournament_name = e[0].text.strip()

            e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                     "/tr[@class='tourney-result with-icons']"
                                     "/td[@class='tourney-details-table-wrapper']/table/tbody/tr"
                                     "/td[@class='tourney-details'][2]/div[@class='info-area']"
                                     "/div[@class='item-details']")
            tournament_ground_type = e[0].text.strip() + " " + e[0][0].text.strip()
            if tournament_ground_type not in tournament_ground_types:
                tournament_ground_types[tournament_ground_type] = {
                    'id': None,
                    'name': tournament_ground_type
                }
                tournament_ground_id = may_insert_ground(dbInstance, tournament_ground_types[tournament_ground_type])
                tournament_ground_types[tournament_ground_type]['id'] = tournament_ground_id
            else:
                tournament_ground_id = tournament_ground_types[tournament_ground_type]['id']

            e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                     "/tr[@class='tourney-result with-icons']/td[@class='title-content']"
                                     "/span[@class='tourney-dates']")
            tournament_date_string = e[0].text.strip().split(" - ")[0]
            tournament_date = datetime.datetime.strptime(tournament_date_string, "%Y.%m.%d")
            tournament_name_full = tournament_name + " " + str(tournament_date.year)
            if tournament_name_full not in tournaments:
                tournaments[tournament_name_full] = {
                    'id': None,
                    'name': tournament_name,
                    'year': tournament_date.year,
                    'ground_type': tournament_ground_id
                }
                tournament_id = may_insert_tournament(dbInstance, tournaments[tournament_name_full])
                tournaments[tournament_name_full]['id'] = tournament_id
            else:
                tournament_id = tournaments[tournament_name_full]['id']

            matches_tree = tournamentTree.xpath("./table[@class='mega-table']/tbody/tr")
            for matchTree in matches_tree:
                opponent_tree = matchTree.xpath("./td[3]/div[1]/a")
                if len(opponent_tree) == 0:
                    continue
                opponent_url = baseUrl + opponent_tree[0].attrib['href']
                opponent = get_player_by_url(opponent_url, players)

                set_tree = matchTree.xpath("./td[5]/a")
                if len(set_tree) == 0:
                    continue

                sets = []
                sets_str = set_tree[0].text.strip().split(" ")
                for setStr in sets_str:
                    if len(setStr) == 2:
                        new_set = {
                            'id': None,
                            'player1_score': int(setStr[0]),
                            'player2_score': int(setStr[1]),
                            'player1_tie_break': 0,
                            'player2_tie_break': 0
                        }
                    else:
                        new_set = {
                            'id': None,
                            'player1_score': -1,
                            'player2_score': -1,
                            'player1_tie_break': 0,
                            'player2_tie_break': 0
                        }
                    new_set['id'] = insert_set(dbInstance, new_set)
                    sets.append(new_set)

                if opponent is None:
                    print("Unknown player: ", opponent_url)

                match = {
                    'id': None,
                    'player1_id': player['id'],
                    'player2_id': opponent['id'] if opponent is not None else None,
                    'set1_id': sets[0]['id'],
                    'set2_id': sets[1]['id'] if len(sets) > 1 else None,
                    'set3_id': sets[2]['id'] if len(sets) > 2 else None,
                    'set4_id': sets[3]['id'] if len(sets) > 3 else None,
                    'set5_id': sets[4]['id'] if len(sets) > 4 else None,
                    'date': tournament_date,
                    'tournament_id': tournament_id,
                    'sets': sets
                }
                match['id'] = insert_match(dbInstance, match)
                matches.append(match)


def main():

    if len(sys.argv) != 4:
        print('Usage: %s [clearDb|downloadPlayers|downloadMatches] begin end' % (sys.argv[0]))
        exit(64)

    operation = sys.argv[1]
    begin = int(sys.argv[2])
    end = int(sys.argv[3])

    if operation == "clearDb":
        clear_db(dbInstance)
        exit(0)

    elif operation == "downloadPlayers":
        download_players(begin, end)
        exit(0)

    elif operation == "clearMatches":
        clear_matches(dbInstance)
        exit(0)

    elif operation == "downloadMatches":
        download_matches(begin, end)
        exit(0)
    else:
        exit(64)

main()
