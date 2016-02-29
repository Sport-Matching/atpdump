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

if len(sys.argv) != 3:
    print("Usage: atpworldtour begin end")
    exit(1)

begin = int(sys.argv[1])
end = int(sys.argv[2])

def connect_sql():
    conn_string = "host='" + host + "' port='" + str(port) + "' dbname='" + db +\
                  "' user='" + user + "' password='" + password + "'"
    try:
        return psycopg2.connect(conn_string)
    except ValueError:
        print("Unable to connect to the database")


def insert(conn, table, name, birthDate, sex, country, weight, size):
    cur = conn.cursor()
    try:
        cur.execute("insert into " + table + " (name, birthdate, sex, country, weight, size)"
                    "VALUES (%s, %s, %s, %s, %s, %s);", (name, birthDate, sex, country, weight, size))
    except Exception as e:
        print("Error " + e)
    cur.close()
    conn.commit()

dbInstance = connect_sql()

session = requests.session()


baseUrl = 'http://www.atpworldtour.com'
getRankingUrl = baseUrl + '/en/rankings/singles/?rankDate=2016-2-29&countryCode=all&rankRange=' +\
                str(begin) + '-' + str(end)

allPlayersHtml = session.get(getRankingUrl)
allPlayersTree = html.fromstring(allPlayersHtml.text)
allPlayersTreeElements = allPlayersTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                              "/div[@id='mainContainer']/div[@id='mainContent']"
                                              "/div[@id='singlesRanking']/div[@id='rankingDetailAjaxContainer']"
                                              "/table[@class='mega-table']/tbody/tr")

i = begin
for playerTreeElement in allPlayersTreeElements:
    nameElement = playerTreeElement[3][0]
    playerUrl = baseUrl + nameElement.attrib['href']
    playerHtml = session.get(playerUrl)
    playerTree = html.fromstring(playerHtml.text)

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                         "/div[@class='inner-wrap']/table/tr[1]/td[1]/div[@class='wrap']"
                         "/div[@class='table-big-value']/span[@class='table-birthday-wrapper']/span")
    if len(e) > 0:
        birthDateString = e[0].text.strip()
    else:
        birthDateString = "(1970.01.01)"
    birthDate = datetime.datetime.strptime(birthDateString, "(%Y.%m.%d)")

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                         "/div[@class='inner-wrap']/div[@class='player-profile-hero-ranking']"
                         "/div[@class='player-flag']/div[@class='player-flag-code']")
    if len(e) > 0:
        country = e[0].text.strip()
    else:
        country = ""

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
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

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
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

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                         "/div[@class='inner-wrap']/div[@class='player-profile-hero-name']")
    i += 1
    if len(e) > 0 and len(e[0]) > 1:
        name = e[0][0].text + " " + e[0][1].text
        print(i, birthDate, country, weight, size, name)
        insert(dbInstance, "players", name, birthDate, '0', country, weight, size)
