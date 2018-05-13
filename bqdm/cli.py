#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import sys

import click
import yaml
from past.types import unicode

import bqdm.message as msg
from bqdm import CONTEXT_SETTINGS
from bqdm.action.dataset import DatasetAction
from bqdm.action.table import SchemaMigrationMode, TableAction
from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from bqdm.util import (list_local_datasets, list_local_tables,
                       str_representer, tuple_representer)

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler(sys.stdout))
_logger.setLevel(logging.INFO)


yaml.add_representer(str, str_representer)
yaml.add_representer(unicode, str_representer)
yaml.add_representer(tuple, tuple_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessEntry, BigQueryAccessEntry.represent)
yaml.add_representer(BigQueryTable, BigQueryTable.represent)
yaml.add_representer(BigQuerySchemaField, BigQuerySchemaField.represent)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--credential_file', '-c', type=click.Path(exists=True), required=False,
              help=msg.HELP_OPTION_CREDENTIAL_FILE)
@click.option('--project', '-p', type=str, required=False,
              help=msg.HELP_OPTION_PROJECT)
@click.option('--debug', is_flag=True, default=False,
              help=msg.HELP_OPTION_DEBUG)
@click.pass_context
def cli(ctx, credential_file, project, debug):
    ctx.obj = dict()
    ctx.obj['credential_file'] = credential_file
    ctx.obj['project'] = project
    ctx.obj['debug'] = debug
    if debug:
        _logger.setLevel(logging.DEBUG)


@cli.command(help=msg.HELP_COMMAND_EXPORT)
@click.option('--output_dir', '-o', type=str, required=True, help=msg.HELP_OPTION_OUTPUT_DIR)
@click.pass_context
def export(ctx, output_dir):
    # TODO dataset option
    # TODO exclude dataset option
    # TODO remove -o option -> argument PATH
    dataset_action = DatasetAction(ctx.obj['credential_file'],
                                   ctx.obj['project'],
                                   ctx.obj['debug'])
    datasets = dataset_action.export(output_dir)
    for dataset in datasets:
        table_action = TableAction(dataset.dataset_id,
                                   credential_file=ctx.obj['credential_file'],
                                   project=ctx.obj['project'])
        table_action.export(output_dir)


@cli.command(help=msg.HELP_COMMAND_PLAN)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_OPTION_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False,
              help=msg.HELP_OPTION_DETAILED_EXIT_CODE)
@click.pass_context
def plan(ctx, conf_dir, detailed_exitcode):
    # TODO dataset option
    # TODO exclude dataset option
    # TODO remove conf_dir option -> argument PATH
    click.echo(msg.MESSAGE_PLAN_HEADER)

    dataset_action = DatasetAction(ctx.obj['credential_file'],
                                   ctx.obj['project'],
                                   ctx.obj['debug'])
    source_datasets = dataset_action.list_datasets()
    target_datasets = list_local_datasets(conf_dir)

    add_dataset_count = dataset_action.plan_add(source_datasets, target_datasets)
    change_dataset_count = dataset_action.plan_change(source_datasets, target_datasets)
    destroy_dataset_count = dataset_action.plan_destroy(source_datasets, target_datasets)

    add_table_count, change_table_count, destroy_table_count = 0, 0, 0
    # TODO ThreadPoolExecutor
    for dataset in target_datasets:
        target_tables = list_local_tables(conf_dir, dataset.dataset_id)
        if target_tables is not None:
            table_action = TableAction(dataset.dataset_id,
                                       credential_file=ctx.obj['credential_file'],
                                       project=ctx.obj['project'],
                                       debug=ctx.obj['debug'])
            source_tables = table_action.list_tables()
            add_table_count += table_action.plan_add(source_tables, target_tables)
            change_table_count += table_action.plan_change(source_tables, target_tables)
            destroy_table_count += table_action.plan_destroy(source_tables, target_tables)

    # TODO dataset & table summary
    if add_dataset_count == 0 and change_dataset_count == 0 and destroy_dataset_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_PLAN_SUMMARY.format(
            add_dataset_count, change_dataset_count, destroy_dataset_count))
        click.echo()
        if detailed_exitcode:
            sys.exit(2)


@cli.command(help=msg.HELP_COMMAND_APPLY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_OPTION_CONF_DIR)
@click.option('--mode', '-m', type=click.Choice([
    SchemaMigrationMode.SELECT_INSERT,
    SchemaMigrationMode.SELECT_INSERT_BACKUP,
    SchemaMigrationMode.REPLACE,
    SchemaMigrationMode.REPLACE_BACKUP,
    SchemaMigrationMode.DROP_CREATE
]), required=True, help=msg.HELP_OPTION_MIGRATION_MODE)
@click.option('--backup_dataset', '-b', type=str, required=False,
              help=msg.HELP_OPTION_BACKUP_DATASET)
@click.pass_context
def apply(ctx, conf_dir, mode, backup_dataset):
    # TODO dataset option
    # TODO exclude dataset option
    # TODO remove conf_dir option -> argument PATH
    dataset_action = DatasetAction(ctx.obj['credential_file'],
                                   ctx.obj['project'],
                                   ctx.obj['debug'])
    source_datasets = dataset_action.list_datasets()
    target_datasets = list_local_datasets(conf_dir)

    add_dataset_count = dataset_action.add(source_datasets, target_datasets)
    change_dataset_count = dataset_action.change(source_datasets, target_datasets)
    destroy_dataset_count = dataset_action.destroy(source_datasets, target_datasets)

    add_table_count, change_table_count, destroy_table_count = 0, 0, 0
    # TODO ThreadPoolExecutor
    for dataset in target_datasets:
        target_tables = list_local_tables(conf_dir, dataset.dataset_id)
        if target_tables is not None:
            table_action = TableAction(dataset.dataset_id, mode, backup_dataset,
                                       credential_file=ctx.obj['credential_file'],
                                       project=ctx.obj['project'],
                                       debug=ctx.obj['debug'])
            source_tables = table_action.list_tables()
            add_table_count += table_action.add(source_tables, target_tables)
            change_table_count += table_action.change(source_tables, target_tables)
            destroy_table_count += table_action.destroy(source_tables, target_tables)

    # TODO dataset & table summary
    if add_dataset_count == 0 and change_dataset_count == 0 and destroy_dataset_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.secho(msg.MESSAGE_APPLY_SUMMARY.format(
            add_dataset_count, change_dataset_count, destroy_dataset_count))
        click.echo()


@cli.group(help=msg.HELP_COMMAND_DESTROY)
@click.pass_context
def destroy(ctx):
    pass


@destroy.command('plan', help=msg.HELP_COMMAND_PLAN_DESTROY)
@click.option('--conf_dir', '-d', type=click.Path(exists=True, file_okay=False), required=True,
              help=msg.HELP_OPTION_CONF_DIR)
@click.option('--detailed_exitcode', is_flag=True, default=False,
              help=msg.HELP_OPTION_DETAILED_EXIT_CODE)
@click.pass_context
def plan_destroy(ctx, conf_dir, detailed_exitcode):
    # TODO dataset option
    # TODO exclude dataset option
    # TODO remove conf_dir option -> argument PATH
    dataset_action = DatasetAction(ctx.obj['credential_file'],
                                   ctx.obj['project'],
                                   ctx.obj['debug'])
    source_datasets = dataset_action.list_datasets()
    target_datasets = list_local_datasets(conf_dir)

    click.echo(msg.MESSAGE_PLAN_HEADER)
    destroy_count = dataset_action.plan_intersection_destroy(source_datasets, target_datasets)
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
              help=msg.HELP_OPTION_CONF_DIR)
@click.pass_context
def apply_destroy(ctx, conf_dir):
    # TODO dataset option
    # TODO exclude dataset option
    # TODO remove conf_dir option -> argument PATH
    dataset_action = DatasetAction(ctx.obj['credential_file'],
                                   ctx.obj['project'],
                                   ctx.obj['debug'])
    source_datasets = dataset_action.list_datasets()
    target_datasets = list_local_datasets(conf_dir)

    destroy_count = dataset_action.intersection_destroy(source_datasets, target_datasets)
    if destroy_count == 0:
        click.secho(msg.MESSAGE_SUMMARY_NO_CHANGE)
        click.echo()
    else:
        click.echo(msg.MESSAGE_APPLY_DESTROY_SUMMARY.format(destroy_count))
        click.echo()


if __name__ == '__main__':
    cli()
