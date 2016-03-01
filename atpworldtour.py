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


def insert_player(conn, player):
    cur = conn.cursor()
    cur.execute("insert into players (name, birthdate, sex, country, weight, size) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;", (player['name'], player['birthDate'], player['sex'],
                                                                  player['country'], player['weight'], player['size']))
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


dbInstance = connect_sql()

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

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='currentTabContent']"
                                 "/div[3]/div[@class='activity-tournament-table'][1]"
                                 "/table[@class='tourney-results-wrapper']/tbody/tr[@class='tourney-result with-icons']"
                                 "/td[@class='title-content']/a[@class='tourney-title']")
    tournament_name = e[0].text.strip()

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='currentTabContent']"
                                 "/div[3]/div[@class='activity-tournament-table'][1]"
                                 "/table[@class='tourney-results-wrapper']/tbody/tr[@class='tourney-result with-icons']"
                                 "/td[@class='tourney-details-table-wrapper']/table/tbody/tr"
                                 "/td[@class='tourney-details'][2]/div[@class='info-area']/div[@class='item-details']")
    tournament_ground_type = e[0].text.strip() + " " + e[0][0].text.strip()
    if tournament_ground_type not in tournament_ground_types:
        tournament_ground_types.append(tournament_ground_type)

    e = playerActivityTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                 "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='currentTabContent']"
                                 "/div[3]/div[@class='activity-tournament-table'][1]"
                                 "/table[@class='tourney-results-wrapper']/tbody/tr[@class='tourney-result with-icons']"
                                 "/td[@class='title-content']/span[@class='tourney-dates']")
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

print(players)
print(tournaments)
print(tournament_ground_types)
