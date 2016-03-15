#!/usr/bin/env bash

rm -rf out/*

./atpworldtour.py downloadYears 2012-01-01 || exit 1

./atpworldtour.py clearDb || exit 2


while read line
do
    date=$(echo "${line}" | cut -d' ' -f1)
    year=$(echo "${date}" | cut -d- -f1)
    count=$(echo "${line}" | cut -d' ' -f2)
    ./download.sh downloadPlayers ${date} ${count}
    mkdir out/players-${year}/
    mv out/players-${year}-* out/players-${year}/
done < ./out/years.txt

while read line
do
    date=$(echo "${line}" | cut -d' ' -f1)
    count=$(echo "${line}" | cut -d' ' -f2)
    ./download.sh downloadMatches ${date} ${count}
done < ./out/years.txt