#! /usr/bin/env bash

# first element to process, inclusive
first=1
# last element to process, inclusive
last=2259
# number of parallel instances to launch
threads=100
# max number of element to process per parallel instance, optional (automatically computed, all items processed at once)
#parallelNbItems=10

preLaunch()
{
    ./atpworldtour.py clearMatches 0 0
}

launchInstance()
{
    local instanceBegin=${1}
    local instanceEnd=${2}
    local instanceCount=${3}
    local threadNumber=${4}
    local instanceNumber=${5}
    ./atpworldtour.py downloadMatches ${instanceBegin} ${instanceEnd}
    #sleep $((${RANDOM} % 10))
}

#==============================================================================

. ./parallel_jobs.sh