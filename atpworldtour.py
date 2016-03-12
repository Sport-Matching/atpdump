#! /usr/bin/env python3

# for i in {0..30}; do ./atpworldtour.py $((i*100)) $((i*100+99))& done

import sys
import re
import datetime
from lxml import html
import requests
import psycopg2

host = "127.0.0.1"
port = 5432
db = "sport_matching"
user = "dev"
password = "dev"

if len(sys.argv) == 3:
    begin = int(sys.argv[1])
    end = int(sys.argv[2])
else:
    begin = 0
    end = 2


def connect_sql():
    conn_string = "host='" + host + "' port='" + str(port) + "' dbname='" + db + \
                  "' user='" + user + "' password='" + password + "'"
    return psycopg2.connect(conn_string)


def clear_db(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM sets")
    cur.execute("DELETE FROM matches")
    cur.execute("DELETE FROM tournaments")
    cur.execute("DELETE FROM rankings")
    cur.execute("DELETE FROM players")
    cur.close()
    conn.commit()


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


def insert_tournament(conn, tournament):
    cur = conn.cursor()
    cur.execute("insert into tournaments (name, ground_type, year) VALUES (%s, %s, %s) RETURNING id;",
                (tournament['name'], tournament['ground_type'], tournament['year']))
    entity_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return entity_id


def insert_set(conn, set):
    cur = conn.cursor()
    cur.execute("insert into sets (player1_score, player2_score, player1_tie_break, player2_tie_break)"
                "VALUES (%s, %s, %s, %s) RETURNING id;",
                (set['player1_score'], set['player1_score'], set['player1_tie_break'], set['player2_tie_break']))
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


def get_player_by_url(url):
    for player_name in players:
        player = players[player_name]
        if player['overviewUrl'] == url or player['activityUrl'] == url:
            return player
    return None


dbInstance = connect_sql()

clear_db(dbInstance)

session = requests.session()

baseUrl = 'http://www.atpworldtour.com'
getRankingUrl = baseUrl + '/en/rankings/singles/?rankDate=2016-2-29&countryCode=all&rankRange=' + \
                str(begin) + '-' + str(end)

allPlayersHtml = session.get(getRankingUrl)
allPlayersTree = html.fromstring(allPlayersHtml.text)
allPlayersTreeElements = allPlayersTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                              "/div[@id='mainContainer']/div[@id='mainContent']"
                                              "/div[@id='singlesRanking']/div[@id='rankingDetailAjaxContainer']"
                                              "/table[@class='mega-table']/tbody/tr")

players = {}
tournaments = {}
tournament_ground_types = []
matches = []

for playerTreeElement in allPlayersTreeElements:
    nameElement = playerTreeElement[3][0]
    playerOverviewUrl = baseUrl + nameElement.attrib['href']
    playerActivityUrl = playerOverviewUrl[:-8] + "player-activity?year=all&matchType=singles"
    playerActivityHtml = session.get(playerActivityUrl)
    playerActivityTree = html.fromstring(playerActivityHtml.text)

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                                 "/div[@class='inner-wrap']/div[@class='player-profile-hero-name']")
    if len(e) == 0 or len(e[0]) <= 1:
        continue
    name = e[0][0].text.strip() + " " + e[0][1].text.strip()

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                                 "/div[@class='inner-wrap']/table/tr[1]/td[1]/div[@class='wrap']"
                                 "/div[@class='table-big-value']/span[@class='table-birthday-wrapper']/span")
    if len(e) > 0:
        birthDateString = e[0].text.strip()
    else:
        birthDateString = "(1970.01.01)"
    birthDate = datetime.datetime.strptime(birthDateString, "(%Y.%m.%d)")

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                                 "/div[@class='inner-wrap']/div[@class='player-profile-hero-ranking']"
                                 "/div[@class='player-flag']/div[@class='player-flag-code']")
    if len(e) > 0:
        country = e[0].text.strip()
    else:
        country = ""

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-image']/img")
    if len(e) > 0:
        playerPictureUrl = baseUrl + e[0].attrib['src']
    else:
        playerPictureUrl = None

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                                 "/div[@class='inner-wrap']/table/tr[1]/td[3]/div[@class='wrap']"
                                 "/div[@class='table-big-value']/span[@class='table-weight-kg-wrapper']")
    if len(e) > 0:
        weight = e[0].text.strip()
        m = re.search("\(([0-9]+)kg\)", weight)
        weight = int(m.group(1)) * 1000
    else:
        weight = 0

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                                 "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                                 "/div[@class='inner-wrap']/table/tr[1]/td[4]/div[@class='wrap']"
                                 "/div[@class='table-big-value']/span[@class='table-height-cm-wrapper']")
    if len(e) > 0:
        size = e[0].text.strip()
        m = re.search("\(([0-9]+)cm\)", size)
        size = int(m.group(1))
    else:
        size = 0

    players[name] = {
        'id': None,
        'overviewUrl': playerOverviewUrl,
        'activityUrl': playerActivityUrl,
        'activityTree': playerActivityTree,
        'pictureUrl': playerPictureUrl,
        'name': name,
        'birthDate': birthDate,
        'sex': '0',
        'country': country,
        'weight': weight,
        'size': size
    }
    player_id = insert_player(dbInstance, players[name])
    players[name]['id'] = player_id

for player_name in players:
    player = players[player_name]
    playerActivityTree = player["activityTree"]

    tournamentsTree = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                               "/div[@id='mainContainer']/div[@id='mainContent']"
                                               "/div[@id='currentTabContent']/div[3]"
                                               "/div[@class='activity-tournament-table']")
    for tournamentTree in tournamentsTree:
        e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                 "/tr[@class='tourney-result with-icons']/td[@class='title-content']"
                                 "/*[@class='tourney-title']")
        tournament_name = e[0].text.strip()

        e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                 "/tr[@class='tourney-result with-icons']/td[@class='tourney-details-table-wrapper']"
                                 "/table/tbody/tr/td[@class='tourney-details'][2]/div[@class='info-area']"
                                 "/div[@class='item-details']")
        tournament_ground_type = e[0].text.strip() + " " + e[0][0].text.strip()
        if tournament_ground_type not in tournament_ground_types:
            tournament_ground_types.append(tournament_ground_type)

        e = tournamentTree.xpath("./table[@class='tourney-results-wrapper']/tbody"
                                 "/tr[@class='tourney-result with-icons']/td[@class='title-content']"
                                 "/span[@class='tourney-dates']")
        tournamentDateString = e[0].text.strip().split(" - ")[0]
        tournamentDate = datetime.datetime.strptime(tournamentDateString, "%Y.%m.%d")
        tournament_name_full = tournament_name + " " + str(tournamentDate.year)
        if tournament_name_full not in tournaments:
            tournaments[tournament_name_full] = {
                'id': None,
                'name': tournament_name,
                'year': tournamentDate.year,
                'ground_type': tournament_ground_types.index(tournament_ground_type)
            }
            tournament_id = insert_tournament(dbInstance, tournaments[tournament_name_full])
            tournaments[tournament_name_full]['id'] = tournament_id
        else:
            tournament_id = tournaments[tournament_name_full]['id']

        matchesTree = tournamentTree.xpath("./table[@class='mega-table']/tbody/tr")
        for matchTree in matchesTree:
            opponentTree = matchTree.xpath("./td[3]/div[1]/a")
            if len(opponentTree) == 0:
                continue
            opponentUrl = baseUrl + opponentTree[0].attrib['href']
            opponent = get_player_by_url(opponentUrl)

            setTree = matchTree.xpath("./td[5]/a")
            if len(setTree) == 0:
                continue

            sets = []
            setsStr = setTree[0].text.strip().split(" ")
            for setStr in setsStr:
                if len(setStr) == 2:
                    set = {
                        'id': None,
                        'player1_score': int(setStr[0]),
                        'player2_score': int(setStr[1]),
                        'player1_tie_break': 0,
                        'player2_tie_break': 0
                    }
                else:
                    set = {
                        'id': None,
                        'player1_score': -1,
                        'player2_score': -1,
                        'player1_tie_break': 0,
                        'player2_tie_break': 0
                    }
                set['id'] = insert_set(dbInstance, set)
                sets.append(set)

            if opponent is None:
                print("Unknown player: ", opponentUrl)

            match = {
                'id': None,
                'player1_id': player['id'],
                'player2_id': opponent['id'] if opponent is not None else None,
                'set1_id': sets[0]['id'],
                'set2_id': sets[1]['id'] if len(sets) > 1 else None,
                'set3_id': sets[2]['id'] if len(sets) > 2 else None,
                'set4_id': sets[3]['id'] if len(sets) > 3 else None,
                'set5_id': sets[4]['id'] if len(sets) > 4 else None,
                'date': tournamentDate,
                'tournament_id': tournament_id,
                'sets': sets
            }
            match['id'] = insert_match(dbInstance, match)
            matches.append(match)

print(players)
print(tournaments)
print(tournament_ground_types)
