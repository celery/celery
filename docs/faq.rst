============================
 Frequently Asked Questions
============================

Questions
=========

MySQL is throwing deadlock errors, what can I do?
-------------------------------------------------

**Answer:** MySQL has default isolation level set to ``REPEATABLE-READ``,
if you don't really need that, set it to ``READ-COMMITTED``.
You can do that by adding the following to your ``my.cnf``::

    [mysqld]
    transaction-isolation = READ-COMMITTED

For more information about InnoDBs transaction model see `MySQL - The InnoDB
Transaction Model and Locking`_ in the MySQL user manual.

(Thanks to Honza Kral and Anton Tsigularov for this solution)

.. _`MySQL - The InnoDB Transaction Model and Locking`: http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

celeryd is not doing anything, just hanging
--------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.

