Description
-----------

Scraping tool for site `<https://tabletki.ua/>`_ written on GO lang.
Extract ATC tree or all drugs from the site and store into MSSQL database.
In dev mode all date stored in JSON and CSV files.

How to build
============
Build for Linux (build/tabletki binary script will be created):

.. code-block:: bash

    make build

Build for Windows (build/tabletki.exe file will be created):

.. code-block:: bash

    make build-windows

Usage
=====
::

    tabletki [atctree|drugs]

    Subcommands:
        atctree
        drugs

    Flags:
        --version  Displays the program version string.
        -h --help  Displays help with available flag, subcommand, and positional value parameters.
        --prod  Set PRODUCTION mode (save results to MSSQL DB)
        --workers  Number of workers to run scan in parralel (default: 20)
        --csvfile  Name of CSV file where save drugs in debug mode (default: tabletki.csv)
        --jsonfile  Name of JSON file where save ATC tree in debug mode (default: ATC_tree.json)
        --mssqlurl  MSSQL database connection url (default: sqlserver://user:pass@localhost:1433?database=drugs)

Development
===========
For development purpose you can use commands:

.. code-block:: bash

    make update
    make run-atctree
    make run-drugs
