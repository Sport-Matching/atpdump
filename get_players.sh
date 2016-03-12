#! /usr/bin/env bash

# first element to process, inclusive
first=2001
# last element to process, inclusive
last=2259
# number of parallel instances to launch
threads=10
# max number of element to process per parallel instance, optional (automatically computed, all items processed at once)
parallelNbItems=10

launchInstance()
{
    local instanceBegin=${1}
    local instanceEnd=${2}
    local instanceCount=${3}
    local threadNumber=${4}
    local instanceNumber=${5}
    sleep $((${RANDOM} % 10))
}

#==============================================================================

launchThread()
{
    local threadFirst=${1}
    local threadLast=${2}
    local threadCount=${3}
    local threadNumber=${4}

    local instanceNumber=0
    for i in $(seq ${threadFirst} ${parallelNbItems} ${threadLast})
    do
        local instanceBegin=${i}
        local instanceEnd=$((${i} + ${parallelNbItems} - 1))
        if [ ${instanceEnd} -gt ${threadLast} ]
        then
            instanceEnd=${threadLast}
        fi
        local instanceCount=$((${instanceEnd} - ${instanceBegin} + 1))
        echo "Launching thread ${threadNumber} instance ${instanceNumber}: ${instanceBegin} ${instanceEnd} ${instanceCount}"
        launchInstance ${instanceBegin} ${instanceEnd} ${instanceCount} ${threadNumber} ${instanceNumber}
        instanceNumber=$((${instanceNumber} + 1))
    done
}

launch()
{
    local first=$1
    local last=$2
    local threads=$3
    local parallelNbItems=$4
    local itemsCount=$((${last} - ${first} + 1))
    local parallelItems=$((${itemsCount} / ${threads}))
    if [ "${parallelNbItems}" == "" ] || [ ${parallelNbItems} -gt ${parallelItems} ]
    then
        parallelNbItems=${parallelItems}
    fi

    echo "Processing ${itemsCount} elements in ${threads} threads with ${parallelNbItems} by ${parallelNbItems} elements per thread"

    local threadNumber=0
    for i in $(seq ${first} ${parallelItems} ${last})
    do
        local threadFirst=${i}
        local threadLast=$((${i} + ${parallelItems} - 1))
        if [ ${threadLast} -gt ${last} ]
        then
            threadLast=${last}
        fi
        local threadCount=$((${threadLast} - ${threadFirst} + 1))
        echo "Launching thread ${threadNumber}: from ${threadFirst} to ${threadLast} (${threadCount})"
        launchThread ${threadFirst} ${threadLast} ${threadCount} ${threadNumber} &
        threadNumber=$((${threadNumber} + 1))
    done
}

launch ${first} ${last} ${threads} "${parallelNbItems}"