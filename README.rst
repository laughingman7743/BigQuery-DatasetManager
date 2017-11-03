.. image:: https://img.shields.io/pypi/pyversions/BigQuery-DatasetManager.svg
    :target: https://pypi.python.org/pypi/BigQuery-DatasetManager/

.. image:: https://travis-ci.org/laughingman7743/BigQuery-DatasetManager.svg?branch=master
    :target: https://travis-ci.org/laughingman7743/BigQuery-DatasetManager

.. image:: https://codecov.io/gh/laughingman7743/BigQuery-DatasetManager/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/laughingman7743/BigQuery-DatasetManager

.. image:: https://img.shields.io/pypi/l/BigQuery-DatasetManager.svg
    :target: https://github.com/laughingman7743/BigQuery-DatasetManager/blob/master/LICENSE


BigQuery-DatasetManager
=======================

BigQuery-DatasetManager is a simple file-based CLI management tool for `BigQuery Datasets`_.

.. _`BigQuery Datasets`: https://cloud.google.com/bigquery/docs/datasets

Requirements
------------

* Python

  - CPython 2,7, 3,4, 3.5, 3.6

Installation
------------

.. code:: bash

    $ pip install BigQuery-DatasetManager

Resource representation
-----------------------

The resource representation of the dataset is described in `YAML format`_.

.. code:: yaml

    name: billing
    friendly_name: null
    description: null
    default_table_expiration_ms: null
    location: US
    labels:
        foo: bar
    access_entries:
    -   role: OWNER
        entity_type: specialGroup
        entity_id: projectOwners
    -   role: OWNER
        entity_type: userByEmail
        entity_id: billing-export-bigquery@system.gserviceaccount.com
    -   role: null
        entity_type: view
        entity_id:
            datasetId: view
            projectId: your-project-id
            tableId: billing_view

.. _`YAML format`: http://www.yaml.org/

See `the official documentation of BigQuery Datasets`_ for details of key names.

+----------------+-------------+-----------+---------+----------------------------------------------------------+
| Key name                                 | Value   | Description                                              |
+================+=============+===========+=========+==========================================================+
| dataset_id                               | str     | Dataset ID.                                              |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| friendly_name                            | str     | Title of the dataset.                                    |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| description                              | str     | Description of the dataset.                              |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| default_table_expiration_ms              | int     | Default expiration time for tables in the dataset.       |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| location                                 | str     | Location in which the dataset is hosted.                 |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| labels                                   | map     | Labels for the dataset.                                  |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| access_entries                           | seq     | Represent grant of an access role to an entity.          |
+----------------+-------------+-----------+---------+----------------------------------------------------------+
| access_entries | role                    | str     | Role granted to the entity. One of                       |
|                |                         |         |                                                          |
|                |                         |         | * ``OWNER``                                              |
|                |                         |         | * ``WRITER``                                             |
|                |                         |         | * ``READER``                                             |
|                |                         |         |                                                          |
|                |                         |         | May also be ``None`` if the ``entity_type`` is ``view``. |
+                +-------------+-----------+---------+----------------------------------------------------------+
|                | entity_type             | str     | Type of entity being granted the role. One of            |
|                |                         |         |                                                          |
|                |                         |         | * ``userByEmail``                                        |
|                |                         |         | * ``groupByEmail``                                       |
|                |                         |         | * ``domain``                                             |
|                |                         |         | * ``specialGroup``                                       |
|                |                         |         | * ``view``                                               |
+                +-------------+-----------+---------+----------------------------------------------------------+
|                | entity_id   |           | str/map | ID of entity being granted the role.                     |
+                +             +-----------+---------+----------------------------------------------------------+
|                |             | datasetId | str     | The ID of the dataset containing this table.             |
|                |             |           |         | (Specified when ``entity_type`` is ``view``.)            |
+                +             +-----------+---------+----------------------------------------------------------+
|                |             | projectId | str     | The ID of the project containing this table.             |
|                |             |           |         | (Specified when ``entity_type`` is ``view``.)            |
+                +             +-----------+---------+----------------------------------------------------------+
|                |             | tableId   | str     | The ID of the table.                                     |
|                |             |           |         | (Specified when ``entity_type`` is ``view``.)            |
+----------------+-------------+-----------+---------+----------------------------------------------------------+

.. _`the official documentation of BigQuery Datasets`: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets

Usage
-----

.. code::

    Usage: bqdm [OPTIONS] COMMAND [ARGS]...

    Options:
      -c, --credential_file PATH  Location of credential file for service accounts.
      --debug                     Debug output management.
      -h, --help                  Show this message and exit.

    Commands:
      apply    Builds or changes datasets.
      destroy  Specify subcommand `plan` or `apply`.
      export   Export existing datasets into file in YAML format.
      plan     Generate and show an execution plan.

Export
~~~~~~

.. code::

    Usage: bqdm export [OPTIONS]

      Export existing datasets into file in YAML format.

    Options:
      -o, --output_dir TEXT  Directory Path to output YAML files.  [required]
      -h, --help             Show this message and exit.

Plan
~~~~

.. code::

    Usage: bqdm plan [OPTIONS]

      Generate and show an execution plan.

    Options:
      -d, --conf_dir DIRECTORY  Directory path where YAML files located.  [required]
      --detailed_exitcode       Return a detailed exit code when the command exits. When provided,
                                this argument changes
                                the exit codes and their meanings to provide
                                more granular information about what the
                                resulting plan contains:
                                0 = Succeeded with empty diff
                                1 = Error
                                2 = Succeeded with non-empty diff
      -h, --help                Show this message and exit.

Apply
~~~~~

.. code::

    Usage: bqdm apply [OPTIONS]

      Builds or changes datasets.

    Options:
      -d, --conf_dir DIRECTORY  Directory path where YAML files located.  [required]
      -h, --help                Show this message and exit.

Destroy
~~~~~~~

.. code::

    Usage: bqdm destroy [OPTIONS] COMMAND [ARGS]...

      Specify subcommand `plan` or `apply`

    Options:
      -h, --help  Show this message and exit.

    Commands:
      apply  Destroy managed datasets.
      plan   Generate and show an execution plan for datasets destruction.

Destroy plan
^^^^^^^^^^^^

.. code::

    Usage: bqdm destroy plan [OPTIONS]

      Generate and show an execution plan for datasets destruction.

    Options:
      -d, --conf_dir DIRECTORY  Directory path where YAML files located.  [required]
      --detailed_exitcode       Return a detailed exit code when the command exits. When provided,
                                this argument changes
                                the exit codes and their meanings to provide
                                more granular information about what the
                                resulting plan contains:
                                0 = Succeeded with empty diff
                                1 = Error
                                2 = Succeeded with non-empty diff
      -h, --help                Show this message and exit.

Destroy apply
^^^^^^^^^^^^^

.. code::

    Usage: bqdm destroy apply [OPTIONS]

      Destroy managed datasets.

    Options:
      -d, --conf_dir DIRECTORY  Directory path where YAML files located.  [required]
      -h, --help                Show this message and exit.

Authentication
--------------

See `authentication section`_ in the official documentation of ``google-cloud-python``.

    If you're running in Compute Engine or App Engine, authentication should "just work".

    If you're developing locally, the easiest way to authenticate is using the Google Cloud SDK:

    .. code:: bash

        $ gcloud auth application-default login

    Note that this command generates credentials for client libraries. To authenticate the CLI itself, use:

    .. code:: bash

        $ gcloud auth login

    Previously, gcloud auth login was used for both use cases. If your gcloud installation does not support the new command, please update it:

    .. code:: bash

        $ gcloud components update

    If you're running your application elsewhere, you should download a service account JSON keyfile and point to it using an environment variable:

    .. code:: bash

        $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"

.. _`authentication section`: https://google-cloud-python.readthedocs.io/en/latest/core/auth.html#overview

Testing
-------

Run test
~~~~~~~~

.. code:: bash

    $ py.test

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install tox
    $ pyenv local 2.7.13 3.4.6 3.5.3 3.6.1
    $ tox

TODO
----

#. Manage table resources
