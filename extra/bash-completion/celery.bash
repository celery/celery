# This is a bash completion script for celery
# Redirect it to a file, then source it or copy it to /etc/bash_completion.d
# to get tab completion. celery must be on your PATH for this to work.
_celery()
{
    local cur basep opts base kval kkey
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    basep="${COMP_WORDS[1]}"
    opts="worker events beat shell multi amqp status
          inspect control purge list migrate call result report"
    fargs="--app= --broker= --loader= --config= --version"
    dopts="--detach --umask= --gid= --uid= --pidfile= --logfile= --loglevel="

    # find the current subcommand, store in basep'
    for index in $(seq 1 $((${#COMP_WORDS[@]} - 2)))
    do
        basep=${COMP_WORDS[$index]}
        if [ "${basep:0:2}" != "--" ]; then
            break;
        fi
    done

    if [ "${cur:0:2}" == "--" -a "$cur" != "${cur//=}" ]; then
        kkey="${cur%=*}"
        kval="${cur#*=}"
        case "${kkey}" in
            --uid)
                COMPREPLY=( $(compgen -u -- "$kval") )
                return 0
            ;;
            --gid)
                COMPREPLY=( $(compgen -g -- "$kval") )
                return 0
                ;;
            *)
            ;;
        esac
    fi

    case "${basep}" in
    worker)
        COMPREPLY=( $(compgen -W '--concurrency= --pool= --purge --logfile=
        --loglevel= --hostname= --beat --schedule= --scheduler= --statedb= --events
        --time-limit= --soft-time-limit= --maxtasksperchild= --queues=
        --include= --pidfile= --autoscale= --autoreload --no-execv' -- ${cur} ) )
        return 0
        ;;
    inspect)
        COMPREPLY=( $(compgen -W 'active active_queues ping registered report
        reserved revoked scheduled stats --help' -- ${cur}) )
            return 0
            ;;
    control)
        COMPREPLY=( $(compgen -W 'add_consumer autoscale cancel_consumer
        disable_events enable_events pool_grow pool_shrink
        rate_limit time_limit --help' -- ${cur}) )
            return 0
            ;;
    multi)
        COMPREPLY=( $(compgen -W 'start restart stopwait stop show
        kill names expand get help --quiet --nosplash
        --verbose --no-color --help' -- ${cur} ) )
        return 0
        ;;
    amqp)
        COMPREPLY=( $(compgen -W 'queue.declare queue.purge exchange.delete
        basic.publish exchange.declare queue.delete queue.bind
        basic.get --help' -- ${cur} ))
        return 0
        ;;
    list)
        COMPREPLY=( $(compgen -W 'bindings' -- ${cur} ) )
        return 0
        ;;
    shell)
        COMPREPLY=( $(compgen -W '--ipython --bpython --python
        --without-tasks --eventlet --gevent' -- ${cur} ) )
        return 0
        ;;
    beat)
        COMPREPLY=( $(compgen -W '--schedule= --scheduler=
        --max-interval= $dopts' -- ${cur}  ))
        return 0
        ;;
    events)
        COMPREPLY=( $(compgen -W '--dump --camera= --freq=
        --maxrate= $dopts' -- ${cur}))
        return 0
        ;;
    *)
        ;;
    esac

   COMPREPLY=($(compgen -W "${opts} ${fargs}" -- ${cur}))
   return 0
}
complete -F _celery celery

