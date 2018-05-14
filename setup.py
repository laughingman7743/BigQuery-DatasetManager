#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from __future__ import print_function
import codecs

from setuptools import setup

import bqdm


with codecs.open('README.rst', 'rb', 'utf-8') as readme:
    long_description = readme.read()


setup(
    name='BigQuery-DatasetManager',
    version=bqdm.__version__,
    description='BigQuery-DatasetManager is a simple file-based CLI management tool '
                'for BigQuery Datasets.',
    long_description=long_description,
    url='https://github.com/laughingman7743/BigQuery-DatasetManager/',
    author='laughingman7743',
    author_email='laughingman7743@gmail.com',
    license='MIT License',
    packages=['bqdm'],
    package_data={
        '': ['*.rst'],
    },
    install_requires=[
        'future',
        'pytz',
        'click>=6.0',
        'PyYAML>=3.12',
        'google-cloud-bigquery==1.1.0',
        'enum34;python_version<="3.3"',
        'python-dateutil>=2.7.0',
    ],
    tests_require=[
        'pytest>=3.5',
        'pytest-cov',
        'pytest-flake8>=1.0.1',
    ],
    entry_points={
        'console_scripts': [
            'bqdm = bqdm.cli:cli',
        ],
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
