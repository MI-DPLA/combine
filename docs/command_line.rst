************
Command Line
************

Though Combine is designed primarily as a GUI interface, the command line provides a powerful and rich interface to the models and methods that make up the Combine data model.  This documentation is meant to expose some of those patterns and conventions.

There are two primary command line contexts:

  - **Django shell**: A shell that loads all Django models, with some additional methods for interacting with Jobs, Records, etc.
  - **Pyspark shell**: A pyspark shell that is useful for interacting with Jobs and Records via a spark context.  

These are described in more detail below.

**Note:** For both, the Combine `Miniconda <https://conda.io/miniconda.html>`__ python environement must be used, which can be activated from any filepath location by
typing:

.. code-block:: bash

    source activate combine


Django Python Shell
===================


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./runconsole.py


Pyspark Shell
=============

The pyspark shell requires the Hadoop Datanode and Namenode to be active.  These are likely running by defult, but in the event they are not, they can be started with the following (Note: the trailing ``:`` is required, as that indicates a group of processes in `Supervisor <http://supervisord.org/>`_):

.. code-block:: bash

    sudo supervisorctl restart hdfs:


Starting
--------

From the location ``/opt/combine`` run the following:

.. code-block:: bash

    ./pyspark_shell.sh

