==============
How To Install
==============

Install in Sphinx
-----------------

Copy this directory into the ``sphinx/templates`` directory where Shpinx is installed. For example, a standard install of sphinx on Mac OS X is at ``/Library/Python/2.6/site-packages/Sphinx-0.6.3-py2.6.egg/``

Install Somewhere Else
----------------------

If you want to install this theme somewhere else, you will have to modify the ``conf.py`` file. ::

    templates_path = ['/absolute/path/to/dir/','relative/path/']

Making Sphinx Use the Theme
---------------------------

Edit the ``conf.py`` file and make the following setting: ::

    html_theme = 'ADCtheme'

Screen Shots
------------

.. image:: http://github.com/coordt/ADCtheme/raw/master/static/scrn1.png

.. image:: http://github.com/coordt/ADCtheme/raw/master/static/scrn2.png

To Do
-----

 * Gotta get the javascript working so the Table of Contents is hide-able.
 * Probably lots of css cleanup.