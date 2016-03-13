#!/usr/bin/env bash

rm -f out/*

./atpworldtour.py downloadYears || exit 1

./atpworldtour.py clearDb || exit 1

for operation in downloadPlayers downloadMatches
do
    while read line
    do
        date=$(echo "${line}" | cut -d' ' -f1)
        count=$(echo "${line}" | cut -d' ' -f2)
        ./download.sh ${operation} ${date} ${count}
    done < ./out/years.txt
done