Usage
=====

The three files should be located as follows respectively (change the filename if needed):

- ``/etc/systemd/system/celery.service``
- ``/etc/systemd/system/celery.service.d/celery.conf``
- ``/etc/tmpfiles.d/celery.conf``

After that, modify the content of the files suitable for your application, and execute the commands:

.. code-block:: bash

  # Create the temporary directories immediately.
  sudo systemd-tmpfiles --create
  
  # Reload the systemd services.
  sudo systemctl daemon-reload

  # Start the service.
  sudo systemctl start celery.service

Note
====

The command used in the script is ``celery worker`` instead of ``celery multi start`` since systemd seeems unable to capture the multiple parent PIDs. Please refer to https://github.com/celery/celery#3459 for more discussion.
