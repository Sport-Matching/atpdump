#! /usr/bin/env python3

import re
import datetime
from lxml import html
import requests
import psycopg2


def connect_sql():
    conn_string = "host='192.168.99.100' port='9999' dbname='music' user='postgres' password='postgres'"
    try:
        return psycopg2.connect(conn_string)
    except ValueError:
        print("Connection impossible to the Database")


def insert(conn, name, style, channel_name, channel_url, verified):
    cur = conn.cursor()
    try:
        cur.execute("insert into musics (name, style, channel_name, channel_url, verified) VALUES (%s, %s, %s, %s, %s);",
                (name, style, channel_name, channel_url, verified))
    except Exception:
        print("bad " + name)
    cur.close()
    conn.commit()

session = requests.session()


baseUrl = 'http://www.atpworldtour.com'
getRankingUrl = baseUrl + '/en/rankings/singles/?rankDate=2016-2-29&countryCode=all&rankRange=0-1'

allPlayersHtml = session.get(getRankingUrl)
allPlayersTree = html.fromstring(allPlayersHtml.text)
allPlayersTreeElements = allPlayersTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                                              "/div[@id='mainContainer']/div[@id='mainContent']"
                                              "/div[@id='singlesRanking']/div[@id='rankingDetailAjaxContainer']"
                                              "/table[@class='mega-table']/tbody/tr")

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
    birthDateString = e[0].text.strip()
    birthDate = datetime.datetime.strptime(birthDateString, "(%Y.%m.%d)")

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                         "/div[@class='inner-wrap']/div[@class='player-profile-hero-ranking']"
                         "/div[@class='player-flag']/div[@class='player-flag-code']")
    country = e[0].text.strip()

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                         "/div[@class='inner-wrap']/table/tr[1]/td[3]/div[@class='wrap']"
                         "/div[@class='table-big-value']/span[@class='table-weight-kg-wrapper']")
    weight = e[0].text.strip()
    m = re.search("\(([0-9]+)kg\)", weight)
    weight = int(m.group(1)) * 1000

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-table']"
                         "/div[@class='inner-wrap']/table/tr[1]/td[4]/div[@class='wrap']"
                         "/div[@class='table-big-value']/span[@class='table-height-cm-wrapper']")
    length = e[0].text.strip()
    m = re.search("\(([0-9]+)cm\)", length)
    length = int(m.group(1))

    e = playerTree.xpath("/html/body/div[@id='mainLayoutWrapper']/div[@id='backbonePlaceholder']"
                         "/div[@id='mainContainer']/div[@id='mainContent']/div[@id='playerProfileHero']"
                         "/div[@class='player-profile-hero-overflow']/div[@class='player-profile-hero-dash']"
                         "/div[@class='inner-wrap']/div[@class='player-profile-hero-name']")
    name = e[0][0].text + " " + e[0][1].text

    print(birthDate, country, weight, length, name)
