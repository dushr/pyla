pyla
====

.. image:: https://travis-ci.org/dushyant88/pyla.png 

.. image:: https://coveralls.io/repos/dushyant88/pyla/badge.svg?branch=master
        :target: https://coveralls.io/r/dushyant88/pyla?branch=master

pyla is a redis based storage system which can filter entries based on item's
attributes. 

In it's current version it stores the information of the entry in a sorted set
with all information present in the key.

Why pyla?
---------

Say you have certain jobs that you want to schedule and every job has certain
attributes associated to it.

.. code-block:: python

    from pyla import entries
    from pyla import fields

    class Job(entries.Entry):
        id = fields.BaseField(primary=True)
        type = fields.BaseField(index=True)
        assigned_to = fields.BaseField(index=True)
        info = fields.BaseField(index=False)

    j = Job(id=1, type='create', assigned_to='dush', info='testing')
    j.save()

on calling save on that particular entry you will have following sorted
sets available in redis

.. code-block::

    job
    job:type:create
    job:assigned_to:dush

Then you have the following awesome filtering abilities:

.. code-block:: python

    # Get all the jobs
    Job.objects.all() 

    # Get jobs with type create
    Job.objects.filter(type='create') 

    # Get jobs with either create or delete type
    Job.objects.filter(type=['create', 'delete']) 

    # Get jobs which are assigened to dush
    Job.objects.filter(assigned_to='dush') 

    # Get jobs which are assigened to dush or spam
    Job.objects.filter(assigned_to=['dush', 'spam'])

    # Get jobs which are assigened to dush and are of type create
    Job.objects.filter(assigned_to='dush', type='create')

    # Get jobs which are assigened to dush and are of type create or delete
    Job.objects.filter(assigned_to='dush', type=['create','delete'])

    # Get jobs which are assigened to dush or spam and are of type create or delete
    Job.objects.filter(assigned_to=['dush', 'spam'], type=['create','delete'])

You can have any number of filters over the fields which were set to index.
Obviously, the more indexes you have the slower your writes become.
