#! /usr/bin/env bash

# first element to process, inclusive
first=1
# last element to process, inclusive
last=${first}
# number of parallel instances to launch
threads=100
# max number of element to process per parallel instance, optional (automatically computed, all items processed at once)
#parallelNbItems=10

usage()
{
    echo "Usage: ${0} downloadPlayers|downloadMatches date count" >&2
    exit 64
}

if [ $# -ne 3 ]
then
    usage
fi

operation=${1}
if [ "${operation}" != "downloadPlayers" ] && [ "${operation}" != "downloadMatches" ]
then
    usage
fi
date=${2}
year=$(echo "${date}" | cut -d- -f1)
last=${3}

preLaunch()
{
    if [ "${operation}" == "downloadPlayers" ]
    then
        rm -f out/players-${year}-*.json
    fi
}

postLaunch()
{
    if [ "${operation}" == "downloadPlayers" ]
    then
        ./atpworldtour.py concatPlayers
    fi
}

launchInstance()
{
    local instanceBegin=${1}
    local instanceEnd=${2}
    local instanceCount=${3}
    local threadNumber=${4}
    local instanceNumber=${5}
    ./atpworldtour.py ${operation} ${date} ${instanceBegin} ${instanceEnd}
}

#==============================================================================

. ./parallel_jobs.sh