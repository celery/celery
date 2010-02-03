#!/bin/bash
#---------------------------------------------------------------------------#
# 
# Tool to find race conditions in the Periodic Task system.
# Outputs times of all runs of a certain task (by searching for task name
# using a search query).
#
# Usage:
#   
#   $ bash periodic-task-runtimes.sh query host1 [host2 ... hostN]
#
# Example usage:
#
#   $ bash periodic-task-runtimes.sh refresh_all_feeds host1 host2 host3
#
# The output is sorted.
#
#---------------------------------------------------------------------------#

USER="root"
CELERYD_LOGFILE="/var/log/celeryd.log"

query="$1"
shift
hosts="$*"

usage () {
    echo "$(basename $0) task_name_query host1 [host2 ... hostN]"
    exit 1
}

[ -z "$query" -o -z "$hosts" ] && usage


get_received_date_for_task () {
    host="$1"
    ssh "$USER@$host" "
        grep '$query' $CELERYD_LOGFILE | \
            grep 'Got task from broker:' | \
            perl -nle'
                /^\[(.+?): INFO.+?Got task from broker:(.+?)\s*/;
                print \"[\$1] $host \$2\"' | \
            sed 's/\s*$//'
    "
}

get_processed_date_for_task () {
    host="$1"
    ssh "$USER@$host" "
        grep '$query' $CELERYD_LOGFILE | \
            grep 'processed:' | \
            perl -nle'
                /^\[(.+?): INFO.+?Task\s+(.+?)\s*/;
                print \"[\$1] $host \$2\"' | \
            sed 's/\s*$//'
    "
}

get_processed_for_all_hosts () {
    for_hosts="$*"
    for host in $for_hosts; do
        get_processed_date_for_task $host
    done
}

get_processed_for_all_hosts $hosts | sort
