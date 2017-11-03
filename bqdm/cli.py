#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
import os
import sys

import click
import yaml
from past.types import unicode

import bqdm.message as msg
from bqdm import CONTEXT_SETTINGS
from bqdm.action import DatasetAction
from bqdm.model import BigQueryDataset, BigQueryAccessEntry
from bqdm.util import list_local_datasets, ordered_dict_constructor, str_representer


_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


yaml.add_representer(str, str_representer)
yaml.add_representer(unicode, str_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessEntry, BigQueryAccessEntry.represent)
yaml.add_constructor('tag:yaml.org,2002:map', ordered_dict_constructor)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--credential_file', '-c', type=click.Path(exists=True), required=False,
              help=msg.HELP_CREDENTIAL_FILE)
@click.option('--debug', is_flag=True, default=False, help=msg.HELP_DEBUG)
@click.pass_context
def cli(ctx, credential_file, debug):
    ctx.obj = dict()
    ctx.obj['credential_file'] = credential_file
    if credential_file:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file
    ctx.obj['debug'] = debug
    if debug:
        _logger.setLevel(logging.DEBUG)


@cli.command(help=msg.HELP_COMMAND_EXPORT)
@click.option('--output_dir', '-o', type=str, required=True, help=msg.HELP_OUTPUT_DIR)
@click.pass_context
def export(ctx, output_dir):
    action = DatasetAction(ctx.obj['debug'])
    action.export(output_dir)


@cli.command(help=msg.HELP_COMMAND_PLAN)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False, help=msg.HELP_DETAILED_EXIT_CODE)
@click.pass_context
def plan(ctx, conf_dir, detailed_exitcode):
    action = DatasetAction(ctx.obj['debug'])
    old_datasets = action.list_datasets()
    new_datasets = list_local_datasets(conf_dir)

    # echo header
    click.echo(msg.MESSAGE_PLAN_HEADER)
    # add
    add_count = action.plan_add(old_datasets, new_datasets)
    # change
    change_count = action.plan_change(old_datasets, new_datasets)
    # destroy
    destroy_count = action.plan_destroy(old_datasets, new_datasets)
    # echo summary
    if add_count == 0 and change_count == 0 and destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_PLAN_SUMMARY.format(add_count, change_count, destroy_count))
        click.echo()
        if detailed_exitcode:
            sys.exit(2)


@cli.command(help=msg.HELP_COMMAND_APPLY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.pass_context
def apply(ctx, conf_dir):
    action = DatasetAction(ctx.obj['debug'])
    old_datasets = action.list_datasets()
    new_datasets = list_local_datasets(conf_dir)

    # add
    add_count = action.add(old_datasets, new_datasets)
    # change
    change_count = action.change(old_datasets, new_datasets)
    # destroy
    destroy_count = action.destroy(old_datasets, new_datasets)
    # echo summary
    if add_count == 0 and change_count == 0 and destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.secho(msg.MESSAGE_APPLY_SUMMARY.format(add_count, change_count, destroy_count))
        click.echo()


@cli.group(help=msg.HELP_COMMAND_DESTROY)
@click.pass_context
def destroy(ctx):
    pass


@destroy.command('plan', help=msg.HELP_COMMAND_PLAN_DESTROY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False, help=msg.HELP_DETAILED_EXIT_CODE)
@click.pass_context
def plan_destroy(ctx, conf_dir, detailed_exitcode):
    action = DatasetAction(ctx.obj['debug'])
    old_datasets = action.list_datasets()
    new_datasets = list_local_datasets(conf_dir)

    # echo header
    click.echo(msg.MESSAGE_PLAN_HEADER)
    # destroy
    destroy_count = action.plan_intersection_destroy(old_datasets, new_datasets)
    # echo summary
    if destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_PLAN_DESTROY_SUMMARY.format(destroy_count))
        click.echo()
        if detailed_exitcode:
            sys.exit(2)


@destroy.command('apply', help=msg.HELP_COMMAND_APPLY_DESTROY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_CONF_DIR)
@click.pass_context
def apply_destroy(ctx, conf_dir):
    action = DatasetAction(ctx.obj['debug'])
    old_datasets = action.list_datasets()
    new_datasets = list_local_datasets(conf_dir)

    # destroy
    destroy_count = action.intersection_destroy(old_datasets, new_datasets)
    # echo summary
    if destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_APPLY_DESTROY_SUMMARY.format(destroy_count))
        click.echo()


if __name__ == '__main__':
    cli()
