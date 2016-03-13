# Sport-Matching dump script

## Configure
Modify database settings in atpworldtour.py
```python
host = "127.0.0.1"
port = 5432
db = "sport_matching"
user = "dev"
password = "dev"
```

## Automatic dump
```shell
./auto.sh
```

## Download years
Download years to out/years.txt
```shell
./atpworldtour.py downloadYears
```

## Download players
Must have downloaded years using above script
$date: each line of out/years.txt
```shell
./download.sh downloadPlayers $date
```

## Download matches
Must have downloaded players using above script
```shell
./download.sh downloadPlayers $date
```
