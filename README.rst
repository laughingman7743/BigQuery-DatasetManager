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

The resource representation of the dataset and the table is described in `YAML format`_.

.. _`YAML format`: http://www.yaml.org/

Dataset
~~~~~~~

.. code:: yaml

    name: dataset1
    friendly_name: null
    description: null
    default_table_expiration_ms: null
    location: US
    access_entries:
    -   role: OWNER
        entity_type: specialGroup
        entity_id: projectOwners
    -   role: WRITER
        entity_type: specialGroup
        entity_id: projectWriters
    -   role: READER
        entity_type: specialGroup
        entity_id: projectReaders
    -   role: OWNER
        entity_type: userByEmail
        entity_id: aaa@bbb.gserviceaccount.com
    -   role: null
        entity_type: view
        entity_id:
            datasetId: view1
            projectId: project1
            tableId: table1
    labels:
        foo: bar

+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| Key name                                 | Value   | Description                                                        |
+================+=============+===========+=========+====================================================================+
| dataset_id                               | str     | ID of the dataset.                                                 |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| friendly_name                            | str     | Title of the dataset.                                              |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| description                              | str     | Description of the dataset.                                        |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| default_table_expiration_ms              | int     | Default expiration time for tables in the dataset.                 |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| location                                 | str     | Location in which the dataset is hosted.                           |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| access_entries                           | seq     | Represents grant of an access role to an entity.                   |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| access_entries | role                    | str     | Role granted to the entity.                                        |
|                |                         |         | The following string values are supported:                         |
|                |                         |         |                                                                    |
|                |                         |         | * ``OWNER``                                                        |
|                |                         |         | * ``WRITER``                                                       |
|                |                         |         | * ``READER``                                                       |
|                |                         |         |                                                                    |
|                |                         |         | It may also be ``null`` if the ``entity_type`` is ``view``.        |
+                +-------------+-----------+---------+--------------------------------------------------------------------+
|                | entity_type             | str     | Type of entity being granted the role. One of                      |
|                |                         |         |                                                                    |
|                |                         |         | * ``userByEmail``                                                  |
|                |                         |         | * ``groupByEmail``                                                 |
|                |                         |         | * ``domain``                                                       |
|                |                         |         | * ``specialGroup``                                                 |
|                |                         |         | * ``view``                                                         |
+                +-------------+-----------+---------+--------------------------------------------------------------------+
|                | entity_id   |           | str/map | If the ``entity_type`` is not 'view', the ``entity_id`` is the     |
|                |             |           |         | ``str`` ID of the entity being granted the role. If the            |
|                |             |           |         | ``entity_type`` is 'view', the ``entity_id`` is a ``dict``         |
|                |             |           |         | representing the view from a different dataset to grant access to. |
+                +             +-----------+---------+--------------------------------------------------------------------+
|                |             | datasetId | str     | ID of the dataset containing this table.                           |
|                |             |           |         | (Specifies when ``entity_type`` is ``view``.)                      |
+                +             +-----------+---------+--------------------------------------------------------------------+
|                |             | projectId | str     | ID of the project containing this table.                           |
|                |             |           |         | (Specifies when ``entity_type`` is ``view``.)                      |
+                +             +-----------+---------+--------------------------------------------------------------------+
|                |             | tableId   | str     | ID of the table.                                                   |
|                |             |           |         | (Specifies when ``entity_type`` is ``view``.)                      |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+
| labels                                   | map     | Labels for the dataset.                                            |
+----------------+-------------+-----------+---------+--------------------------------------------------------------------+

NOTE: See `the official documentation of BigQuery Datasets`_ for details of key names.

.. _`the official documentation of BigQuery Datasets`: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets

Table
~~~~~

.. code:: yaml

    table_id: table1
    friendly_name: null
    description: null
    expires: null
    partitioning_type: null
    view_use_legacy_sql: null
    view_query: null
    schema:
    -   name: column1
        field_type: STRING
        mode: REQUIRED
        description: null
        fields: null
    -   name: column2
        field_type: RECORD
        mode: NULLABLE
        description: null
        fields:
        -   name: column2_1
            field_type: STRING
            mode: NULLABLE
            description: null
            fields: null
        -   name: column2_2
            field_type: INTEGER
            mode: NULLABLE
            description: null
            fields: null
        -   name: column2_3
            field_type: RECORD
            mode: REPEATED
            description: null
            fields:
            -   name: column2_3_1
                field_type: BOOLEAN
                mode: NULLABLE
                description: null
                fields: null
    labels:
        foo: bar

.. code:: yaml

    table_id: view1
    friendly_name: null
    description: null
    expires: null
    partitioning_type: null
    view_use_legacy_sql: false
    view_query: |
        select
        *
        from
        `project1.dataset1.table1`
    schema: null
    labels: null

+----------------+--------------+-------+-------------------------------------------------------------------------------+
| Key name                      | Value | Description                                                                   |
+================+==============+=======+===============================================================================+
| table_id                      | str   | ID of the table.                                                              |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| friendly_name                 | str   | Title of the table.                                                           |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| description                   | str   | Description of the table.                                                     |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| expires                       | str   | Datetime at which the table will be deleted.                                  |
|                               |       | (ISO8601 format ``%Y-%m-%dT%H:%M:%S.%f%z``)                                   |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| partitioning_type             | str   | Time partitioning of the table if it is partitioned.                          |
|                               |       | The only partitioning type that is currently supported is ``DAY``.            |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| view_use_legacy_sql           | bool  | Specifies whether to use BigQuery's legacy SQL for this view.                 |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| view_query                    | str   | SQL query defining the table as a view.                                       |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| schema                        | seq   | The schema of the table destination for the row.                              |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| schema         | name         | str   | The name of the field.                                                        |
+                +--------------+-------+-------------------------------------------------------------------------------+
|                | field_type   | str   | The type of the field. One of                                                 |
|                |              |       |                                                                               |
|                |              |       | * ``STRING``                                                                  |
|                |              |       | * ``BYTES``                                                                   |
|                |              |       | * ``INTEGER``                                                                 |
|                |              |       | * ``INT64`` (same as INTEGER)                                                 |
|                |              |       | * ``FLOAT``                                                                   |
|                |              |       | * ``FLOAT64`` (same as FLOAT)                                                 |
|                |              |       | * ``BOOLEAN``                                                                 |
|                |              |       | * ``BOOL`` (same as BOOLEAN)                                                  |
|                |              |       | * ``TIMESTAMP``                                                               |
|                |              |       | * ``DATE``                                                                    |
|                |              |       | * ``TIME``                                                                    |
|                |              |       | * ``DATETIME``                                                                |
|                |              |       | * ``RECORD`` (where RECORD indicates that the field contains a nested schema) |
|                |              |       | * ``STRUCT`` (same as RECORD)                                                 |
+                +--------------+-------+-------------------------------------------------------------------------------+
|                | mode         | str   | The mode of the field. One of                                                 |
|                |              |       |                                                                               |
|                |              |       | * ``NULLABLE``                                                                |
|                |              |       | * ``REQUIRED``                                                                |
|                |              |       | * ``REPEATED``                                                                |
+                +--------------+-------+-------------------------------------------------------------------------------+
|                | description  | str   | Description for the field.                                                    |
+                +--------------+-------+-------------------------------------------------------------------------------+
|                | fields       | seq   | Describes the nested schema fields if the type property is set to ``RECORD``. |
+----------------+--------------+-------+-------------------------------------------------------------------------------+
| labels                        | map   | Labels for the table.                                                         |
+----------------+--------------+-------+-------------------------------------------------------------------------------+

NOTE: See `the official documentation of BigQuery Tables`_ for details of key names.

.. _`the official documentation of BigQuery Tables`: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables

Directory structure
~~~~~~~~~~~~~~~~~~~

.. code::

    .
    ├── dataset1        # Directory storing the table configuration file of dataset1
    │   ├── table1.yml  # Configuration file of table1 in dataset1
    │   └── table2.yml  # Configuration file of table2 in dataset1
    ├── dataset1.yml    # Configuration file of dataset1
    ├── dataset2        # Directory storing the table configuration file of dataset2
    │   └── .gitkeep    # When keeping a directory, dataset2 is empty.
    ├── dataset2.yml    # Configuration file of dataset2
    └── dataset3.yml    # Configuration file of dataset3

NOTE: If you do not want to manage the table, delete the directory with the same name as the dataset name.

Usage
-----

.. code::

    Usage: bqdm [OPTIONS] COMMAND [ARGS]...

    Options:
      -c, --credential-file PATH  Location of credential file for service accounts.
      -p, --project TEXT          Project ID for the project which you’d like to manage with.
      --color / --no-color        Enables output with coloring.
      --parallelism INTEGER       Limit the number of concurrent operation.
      --debug                     Debug output management.
      -h, --help                  Show this message and exit.

    Commands:
      apply    Builds or changes datasets.
      destroy  Specify subcommand `plan` or `apply`
      export   Export existing datasets into file in YAML format.
      plan     Generate and show an execution plan.

Export
~~~~~~

.. code::

    Usage: bqdm export [OPTIONS] [OUTPUT_DIR]

      Export existing datasets into file in YAML format.

    Options:
      -d, --dataset TEXT          Specify the ID of the dataset to manage.
      -e, --exclude-dataset TEXT  Specify the ID of the dataset to exclude from managed.
      -h, --help                  Show this message and exit.

Plan
~~~~

.. code::

    Usage: bqdm plan [OPTIONS] [CONF_DIR]

      Generate and show an execution plan.

    Options:
      --detailed_exitcode         Return a detailed exit code when the command exits.
                                  When provided, this argument changes
                                  the exit codes and their meanings to provide
                                  more granular information about what the
                                  resulting plan contains:
                                  0 = Succeeded with empty diff
                                  1 = Error
                                  2 = Succeeded with non-
                                  empty diff
      -d, --dataset TEXT          Specify the ID of the dataset to manage.
      -e, --exclude-dataset TEXT  Specify the ID of the dataset to exclude from managed.
      -h, --help                  Show this message and exit.

Apply
~~~~~

.. code::

    Usage: bqdm apply [OPTIONS] [CONF_DIR]

      Builds or changes datasets.

    Options:
      -d, --dataset TEXT              Specify the ID of the dataset to manage.
      -e, --exclude-dataset TEXT      Specify the ID of the dataset to exclude from managed.
      -m, --mode [select_insert|select_insert_backup|replace|replace_backup|drop_create|drop_create_backup]
                                      Specify the migration mode when changing the schema.
                                      Choice from `select_insert`,
                                      `select_insert_backup`, `replace`, r`eplace_backup`,
                                      `drop_create`,
                                      `drop_create_backup`.  [required]
      -b, --backup-dataset TEXT       Specify the ID of the dataset to store the backup at migration
      -h, --help                      Show this message and exit.

NOTE: See `migration mode`_

Destroy
~~~~~~~

.. code::

    Usage: bqdm destroy [OPTIONS] COMMAND [ARGS]...

      Specify subcommand `plan` or `apply`

    Options:
      -h, --help  Show this message and exit.

    Commands:
      apply  Destroy managed datasets.
      plan   Generate and show an execution plan for...

Destroy plan
^^^^^^^^^^^^

.. code::

    Usage: bqdm destroy plan [OPTIONS] [CONF_DIR]

      Generate and show an execution plan for datasets destruction.

    Options:
      --detailed-exitcode         Return a detailed exit code when the command exits.
                                  When provided, this argument changes
                                  the exit codes and their meanings to provide
                                  more granular information about what the
                                  resulting plan contains:
                                  0 = Succeeded with empty diff
                                  1 = Error
                                  2 = Succeeded with non-
                                  empty diff
      -d, --dataset TEXT          Specify the ID of the dataset to manage.
      -e, --exclude-dataset TEXT  Specify the ID of the dataset to exclude from managed.
      -h, --help                  Show this message and exit.

Destroy apply
^^^^^^^^^^^^^

.. code::

    Usage: bqdm destroy apply [OPTIONS] [CONF_DIR]

      Destroy managed datasets.

    Options:
      -d, --dataset TEXT          Specify the ID of the dataset to manage.
      -e, --exclude-dataset TEXT  Specify the ID of the dataset to exclude from managed.
      -h, --help                  Show this message and exit.

Migration mode
--------------

select_insert
~~~~~~~~~~~~~

#. TODO

LIMITATIONS: TODO

select_insert_backup
~~~~~~~~~~~~~~~~~~~~

#. TODO

LIMITATIONS: TODO

replace
~~~~~~~

#. TODO

LIMITATIONS: TODO

replace_backup
~~~~~~~~~~~~~~

#. TODO

LIMITATIONS: TODO

drop_create
~~~~~~~~~~~

# TODO

drop_create_backup
~~~~~~~~~~~~~~~~~~

# TODO

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

Depends on the following environment variables:

.. code:: bash

    $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
    $ export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID

Run test
~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pipenv run pytest

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pyenv local 3.6.5 3.5.5 3.4.8 2.7.14
    $ pipenv run tox

TODO
----

#. Support encryption configuration for table
#. Support external data configuration for table
#. Schema replication
#. Integration tests
