import json
import glob
import sys
import re
import datetime
import time
from lxml import html
import requests
import psycopg2

baseUrl = 'http://www.atpworldtour.com'


def connect_sql():
    host = "127.0.0.1"
    port = 43001
    db = "sport_matching"
    user = "postgres"
    password = "postgres"
    conn_string = "host='" + host + "' port='" + str(port) + "' dbname='" + db + \
                  "' user='" + user + "' password='" + password + "'"
    return psycopg2.connect(conn_string)

dbInstance = connect_sql()


def insert_classement(conn, classement, name):
    cur = conn.cursor()
    classement = classement.replace('T', '')
    try:
        cur.execute("insert into rankings (player_id, rank, year)"
                "VALUES ((SELECT id FROM players WHERE name = %s), %s, %s);",
                (name, int(classement), 2016))
        cur.close()
        conn.commit()
    except Exception as aa:
        print(aa.with_traceback(), classement, name)
        pass


#insert_classement(dbInstance, 42, "A Ram Kim")

session = requests.session()
time.sleep(1)
classement = session.get("http://www.atpworldtour.com/en/rankings/singles/?rankDate=2016-5-23&countryCode=all&rankRange=1-10000")
classement_tree = html.fromstring(classement.text)
classement_tree_elements = classement_tree.xpath("//tbody/tr")
for cla in classement_tree_elements:
    rank = cla[0].text.strip()
    name = cla[3][0].text.strip()
    insert_classement(dbInstance, rank, name)



