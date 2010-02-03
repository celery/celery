#!/bin/bash
#--------------------------------------------------------------------#
# Find all currently unprocessed tasks by searching the celeryd
# log file.
#
# Please note that this will also include tasks that raised an exception,
# or is just under active processing (will finish soon).
#
# Usage:
#
#     # Using default log file /var/log/celeryd.log
#     $ bash find-unprocessed-tasks.sh
#
#     # Using a custom logfile
#     # bash find-unprocessed-tasks.sh ./celeryd.log
# 
#--------------------------------------------------------------------#

DEFAULT_LOGFILE=/var/log/celeryd.log
export CELERYD_LOGFILE=${1:-$DEFAULT_LOGFILE}


get_start_date_by_task_id() {
    task_id="$1"
    grep Apply $CELERYD_LOGFILE | \
        grep "$task_id" | \
        perl -nle'
            /^\[(.+?): DEBUG/; print $1' | \
        sed 's/\s*$//'
}


get_end_date_by_task_id() {
    task_id="$1"
    grep processed $CELERYD_LOGFILE | \
        grep "$task_id" | \
        perl -nle'
            /^\[(.+?): INFO/; print $1 ' | \
        sed 's/\s*$//'
}


get_all_task_ids() {
    grep Apply $CELERYD_LOGFILE | perl -nle"/'task_id': '(.+?)'/; print \$1"
}


search_logs_for_task_id() {
    grep "$task_id" $CELERYD_LOGFILE
}


report_unprocessed_task() {
    task_id="$1"
    date_start="$2"

    cat <<EOFTEXT
"---------------------------------------------------------------------------------"
| UNFINISHED TASK: $task_id [$date_start]
"---------------------------------------------------------------------------------"
Related logs:
EOFTEXT
	search_logs_for_task_id "$task_id"
}

for task_id in $(get_all_task_ids); do
    date_start=$(get_start_date_by_task_id "$task_id")
    date_end=$(get_end_date_by_task_id "$task_id")
    if [ -z "$date_end" ]; then
        report_unprocessed_task "$task_id" "$date_start"
    fi
done
