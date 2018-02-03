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

rem - %n will be replaced with the first part of the nodename.
rem - %I will be replaced with the current child process index
rem   and is important when using the prefork pool to avoid race conditions.
set CELERYD_PID_FILE=%PATH_TO_PROJECT%\log\celery.pid
set CELERYD_LOG_FILE=%PATH_TO_PROJECT%\log\celery.log
set CELERYD_LOG_LEVEL=INFO

rem You might need to change th path of the Python runing
set PYTHONPATH=%PYTHONPATH%;%PATH_TO_PROJECT%;

cd %PATH_TO_PROJECT%
del %CELERYD_PID_FILE%
del %CELERYD_LOG_FILE%

%CELERY_BIN% -A %CELERY_APP% worker --loglevel=%CELERYD_LOG_LEVEL% -P eventlet