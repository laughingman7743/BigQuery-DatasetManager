#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import click


HELP_COMMAND_EXPORT = 'Export existing datasets into file in YAML format.'

HELP_COMMAND_PLAN = 'Generate and show an execution plan.'

HELP_COMMAND_APPLY = 'Builds or changes datasets.'

HELP_COMMAND_DESTROY = 'Specify subcommand `plan` or `apply`'

HELP_COMMAND_PLAN_DESTROY = 'Generate and show an execution plan for datasets destruction.'

HELP_COMMAND_APPLY_DESTROY = 'Destroy managed datasets.'

HELP_CREDENTIAL_FILE = 'Location of credential file for service accounts.'

HELP_DEBUG = 'Debug output management.'

HELP_OUTPUT_DIR = 'Directory Path to output YAML files.'

HELP_CONF_DIR = 'Directory path where YAML files located.'

HELP_DETAILED_EXIT_CODE = """Return a detailed exit code when the command exits. When provided,
this argument changes the exit codes and their meanings to provide
more granular information about what the resulting plan contains:
0 = Succeeded with empty diff
1 = Error
2 = Succeeded with non-empty diff"""

MESSAGE_PLAN_HEADER = """An execution plan has been generated and is shown below.

Resource actions are indicated with the following symbols:
{0}
{1}
{2}

BigQuery-DatasetManager will perform the following actions:""".format(
    '  {0} create'.format(click.style('+', fg='green')),
    '  {0} update'.format(click.style('~', fg='yellow')),
    '  {0} destroy'.format(click.style('-', fg='red')))

MESSAGE_PLAN_SUMMARY = 'Plan: {0} to add, {1} to change, {2} to destroy'

MESSAGE_PLAN_DESTROY_SUMMARY = 'Plan: {0} to destroy'

MESSAGE_APPLY_SUMMARY = 'Apply: {0} added, {1} changed, {2} destroyed'

MESSAGE_APPLY_DESTROY_SUMMARY = 'Destroy: {0} destroyed'

MESSAGE_SUMMARY_NO_CHANGE = 'No changes. Dataset is up-to-date.'
