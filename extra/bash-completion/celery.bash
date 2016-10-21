# This is a bash completion script for celery
# Redirect it to a file, then source it or copy it to /etc/bash_completion.d
# to get tab completion. celery must be on your PATH for this to work.
_celery()
{
    local cur basep opts base kval kkey loglevels prevp in_opt controlargs
    local pools
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prevp="${COMP_WORDS[COMP_CWORD-1]}"
    basep="${COMP_WORDS[1]}"
    opts="worker events beat shell multi amqp status
          inspect control purge list migrate call result
          report upgrade flower graph logtool help"
    fargs="--app= --broker= --loader= --config= --version"
    dopts="--detach --umask= --gid= --uid= --pidfile=
           --logfile= --loglevel= --executable="
    controlargs="--timeout --destination"
    pools="prefork eventlet gevent solo"
    loglevels="critical error warning info debug"
    in_opt=0

    # find the current sub-command, store in basep'
    for index in $(seq 1 $((${#COMP_WORDS[@]} - 2)))
    do
        basep=${COMP_WORDS[$index]}
        if [ "${basep:0:2}" != "--" ]; then
            break;
        fi
    done

    if [ "${cur:0:2}" == "--" -a "$cur" != "${cur//=}" ]; then
        in_opt=1
        kkey="${cur%=*}"
        kval="${cur#*=}"
    elif [ "${prevp:0:1}" == "-" ]; then
        in_opt=1
        kkey="$prevp"
        kval="$cur"
    fi

    if [ $in_opt -eq 1 ]; then
        case "${kkey}" in
            --uid|-u)
                COMPREPLY=( $(compgen -u -- "$kval") )
                return 0
            ;;
            --gid|-g)
                COMPREPLY=( $(compgen -g -- "$kval") )
                return 0
            ;;
            --pidfile|--logfile|-p|-f|--statedb|-S|-s|--schedule-filename)
                COMPREPLY=( $(compgen -f -- "$kval") )
                return 0
            ;;
            --workdir)
                COMPREPLY=( $(compgen -d -- "$kval") )
                return 0
            ;;
            --loglevel|-l)
                COMPREPLY=( $(compgen -W "$loglevels" -- "$kval") )
                return 0
            ;;
            --pool|-P)
                COMPREPLY=( $(compgen -W "$pools" -- "$kval") )
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
        --time-limit= --soft-time-limit= --max-tasks-per-child= --queues=
        --include= --pidfile= --autoscale $fargs' -- ${cur} ) )
        return 0
        ;;
    inspect)
        COMPREPLY=( $(compgen -W 'active active_queues ping registered report
        reserved revoked scheduled stats --help $controlargs $fargs' -- ${cur}) )
        return 0
        ;;
    control)
        COMPREPLY=( $(compgen -W 'add_consumer autoscale cancel_consumer
        disable_events enable_events pool_grow pool_shrink
        rate_limit time_limit --help $controlargs $fargs' -- ${cur}) )
        return 0
        ;;
    multi)
        COMPREPLY=( $(compgen -W 'start restart stopwait stop show
        kill names expand get help --quiet --nosplash
        --verbose --no-color --help $fargs' -- ${cur} ) )
        return 0
        ;;
    amqp)
        COMPREPLY=( $(compgen -W 'queue.declare queue.purge exchange.delete
        basic.publish exchange.declare queue.delete queue.bind
        basic.get --help $fargs' -- ${cur} ))
        return 0
        ;;
    list)
        COMPREPLY=( $(compgen -W 'bindings $fargs' -- ${cur} ) )
        return 0
        ;;
    shell)
        COMPREPLY=( $(compgen -W '--ipython --bpython --python
        --without-tasks --eventlet --gevent $fargs' -- ${cur} ) )
        return 0
        ;;
    beat)
        COMPREPLY=( $(compgen -W '--schedule= --scheduler=
        --max-interval= $dopts $fargs' -- ${cur}  ))
        return 0
        ;;
    events)
        COMPREPLY=( $(compgen -W '--dump --camera= --freq=
        --maxrate= $dopts $fargs' -- ${cur}))
        return 0
        ;;
    *)
        ;;
    esac

   COMPREPLY=($(compgen -W "${opts} ${fargs}" -- ${cur}))
   return 0
}
complete -F _celery celery

