#!/usr/bin/env bash

launchThread()
{
    local threadFirst=${1}
    local threadLast=${2}
    local threadCount=${3}
    local threadNumber=${4}
    local parallelNbItems=${5}

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

    if [ ${threads} -gt ${itemsCount} ]
    then
        threads=${itemsCount}
    fi

    local parallelItems=$(( $((${itemsCount} + $((${itemsCount} % ${threads})))) / ${threads}))
    if [ "${parallelNbItems}" == "" ] || [ ${parallelNbItems} -gt ${parallelItems} ]
    then
        parallelNbItems=${parallelItems}
    fi

    echo "Running pre launch"
    preLaunch

    echo "Processing ${itemsCount} elements in ${threads} threads with ${parallelNbItems} by ${parallelNbItems} elements per thread"


    local threadsPid=()
    local threadNumber=0
    # trap ctrl_c INT
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
        launchThread ${threadFirst} ${threadLast} ${threadCount} ${threadNumber} ${parallelNbItems} &
        local threadPid=$!
        threadsPid[${threadNumber}]=${threadPid}
        threadNumber=$((${threadNumber} + 1))
    done

    local stillRunningCount=${#threadsPid[@]}
    local firstRun=1
    while [ ${stillRunningCount} -ne 0 ]
    do
        stillRunningCount=0
        local newFinished=0
        for i in $(seq 0 $((${#threadsPid[@]} - 1)))
        do
            pid=${threadsPid[${i}]}
            if [ "${pid}" == "" ]
            then
                pid=0
            fi
            if [ ${pid} -ne 0 ]
            then
                kill -0 ${pid} > /dev/null 2>/dev/null
                local ret=$?
                if [ ${ret} -eq 1 ]
                then
                    echo "Thread ${i} has finished"
                    newFinished=1
                    threadsPid[${i}]=0
                else
                    stillRunningCount=$((${stillRunningCount} + 1))
                fi
            fi
        done
        if [ ${stillRunningCount} -ne 0 ]
        then
            if [ ${newFinished} -eq 1 ] && [ ${stillRunningCount} -ne 0 ]
            then
                echo "${stillRunningCount} remaining threads"
            fi
            sleep 1
        fi
    done
    echo "All threads done"


    echo "Running post launch"
    postLaunch
}

launch ${first} ${last} ${threads} "${parallelNbItems}"