rem Name of nodes to start
rem here we have a single node
set CELERYD_NODES=w1
rem or we could have three nodes:
rem CELERYD_NODES="w1 w2 w3"

rem App instance to use
rem comment out this line if you don't use an app
set CELERY_APP=proj
rem or fully qualified:
rem CELERY_APP="proj.tasks:app"

set PATH_TO_PROJECT=D:\home\site\wwwroot

rem Absolute or relative path to the 'celery' and 'Python' command:
set CELERY_BIN=%PATH_TO_PROJECT%\env\Scripts\celery

rem How to call manage.py
set CELERYD_MULTI=multi

rem - %n will be replaced with the first part of the nodename.
rem - %I will be replaced with the current child process index
rem   and is important when using the prefork pool to avoid race conditions.
set CELERYD_PID_FILE=%PATH_TO_PROJECT%\log\celerybeat.pid
set CELERYD_LOG_FILE=%PATH_TO_PROJECT%\log\celerybeat.log
set CELERYD_LOG_LEVEL=INFO

rem CONFIG RELATED TO THE BEAT 
set CELERYD_DATABASE=django
set CELERYD_SCHEDULER=django_celery_beat.schedulers:DatabaseScheduler

rem You might need to change th path of the Python runing
set PYTHONPATH=%PYTHONPATH%;%PATH_TO_PROJECT%;

cd %PATH_TO_PROJECT%
del %CELERYD_PID_FILE%
del %CELERYD_LOG_FILE%

%CELERY_BIN% -A %CELERY_APP% beat -S %CELERYD_DATABASE% --logfile=%CELERYD_LOG_FILE% --pidfile=%CELERYD_PID_FILE% --scheduler %CELERYD_SCHEDULER% --loglevel=%CELERYD_LOG_LEVEL%