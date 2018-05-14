#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

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
from bqdm.util import (as_completed, echo, get_parallelism, list_local_datasets, list_local_tables,
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


def _export_table(ctx, dataset_id, output_dir):
    with TableAction(dataset_id,
                     project=ctx.obj['project'],
                     credential_file=ctx.obj['credential_file'],
                     no_color=not ctx.obj['color'],
                     parallelism=ctx.obj['parallelism'],
                     debug=ctx.obj['debug']) as action:
        action.export(output_dir)


def _plan_table(ctx, dataset_id, conf_dir):
    target_tables = list_local_tables(conf_dir, dataset_id)
    add_count, change_count, destroy_count = 0, 0, 0
    if target_tables is not None:
        with TableAction(dataset_id,
                         project=ctx.obj['project'],
                         credential_file=ctx.obj['credential_file'],
                         no_color=not ctx.obj['color'],
                         parallelism=ctx.obj['parallelism'],
                         debug=ctx.obj['debug']) as action:
            source_tables = action.list_tables()
            add_count = action.plan_add(source_tables, target_tables)
            change_count = action.plan_change(source_tables, target_tables)
            destroy_count = action.plan_destroy(source_tables, target_tables)
    return add_count, change_count, destroy_count


def _apply_table(ctx, dataset_id, conf_dir, mode, backup_dataset):
    target_tables = list_local_tables(conf_dir, dataset_id)
    add_count, change_count, destroy_count = 0, 0, 0
    if target_tables is not None:
        with TableAction(dataset_id,
                         migration_mode=mode,
                         backup_dataset_id=backup_dataset,
                         project=ctx.obj['project'],
                         credential_file=ctx.obj['credential_file'],
                         no_color=not ctx.obj['color'],
                         parallelism=ctx.obj['parallelism'],
                         debug=ctx.obj['debug']) as action:
            source_tables = action.list_tables()
            add_count = action.add(source_tables, target_tables)
            change_count = action.change(source_tables, target_tables)
            destroy_count = action.destroy(source_tables, target_tables)
    return add_count, change_count, destroy_count


def _plan_destroy_table(ctx, dataset_id):
    with TableAction(dataset_id,
                     project=ctx.obj['project'],
                     credential_file=ctx.obj['credential_file'],
                     no_color=not ctx.obj['color'],
                     parallelism=ctx.obj['parallelism'],
                     debug=ctx.obj['debug']) as action:
        source_tables = action.list_tables()
        count = action.plan_destroy(source_tables, [])
    return count


def _apply_destroy_table(ctx, dataset_id):
    with TableAction(dataset_id,
                     project=ctx.obj['project'],
                     credential_file=ctx.obj['credential_file'],
                     no_color=not ctx.obj['color'],
                     parallelism=ctx.obj['parallelism'],
                     debug=ctx.obj['debug']) as action:
        source_tables = action.list_tables()
        count = action.destroy(source_tables, [])
    return count


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--credential-file', '-c', type=click.Path(exists=True), required=False,
              help=msg.HELP_OPTION_CREDENTIAL_FILE)
@click.option('--project', '-p', type=str, required=False,
              help=msg.HELP_OPTION_PROJECT)
@click.option('--color/--no-color', default=True, required=False,
              help=msg.HELP_OPTION_COLOR)
@click.option('--parallelism', type=int, required=False, default=get_parallelism(),
              help=msg.HELP_OPTION_PARALLELISM)
@click.option('--debug', is_flag=True, default=False,
              help=msg.HELP_OPTION_DEBUG)
@click.pass_context
def cli(ctx, credential_file, project, color, parallelism, debug):
    ctx.obj = dict()
    ctx.obj['credential_file'] = credential_file
    ctx.obj['project'] = project
    ctx.obj['color'] = color
    ctx.obj['parallelism'] = parallelism
    ctx.obj['debug'] = debug
    if debug:
        _logger.setLevel(logging.DEBUG)


@cli.command(help=msg.HELP_COMMAND_EXPORT)
@click.argument('output-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.pass_context
def export(ctx, output_dir, dataset, exclude_dataset):
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        with DatasetAction(project=ctx.obj['project'],
                           credential_file=ctx.obj['credential_file'],
                           no_color=not ctx.obj['color'],
                           parallelism=ctx.obj['parallelism'],
                           debug=ctx.obj['debug']) as action:
            datasets = action.export(output_dir, dataset, exclude_dataset)

        fs = [e.submit(_export_table, ctx, d.dataset_id, output_dir) for d in datasets]
        as_completed(fs)


@cli.command(help=msg.HELP_COMMAND_PLAN)
@click.argument('conf-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--detailed_exitcode', is_flag=True, default=False,
              help=msg.HELP_OPTION_DETAILED_EXIT_CODE)
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.pass_context
def plan(ctx, conf_dir, detailed_exitcode, dataset, exclude_dataset):
    echo(msg.MESSAGE_PLAN_HEADER)

    add_counts, change_counts, destroy_counts = [], [], []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        with DatasetAction(project=ctx.obj['project'],
                           credential_file=ctx.obj['credential_file'],
                           no_color=not ctx.obj['color'],
                           parallelism=ctx.obj['parallelism'],
                           debug=ctx.obj['debug']) as action:
            source_datasets = action.list_datasets(dataset, exclude_dataset)
            target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)

            add_counts.append(action.plan_add(source_datasets, target_datasets))
            change_counts.append(action.plan_change(source_datasets, target_datasets))
            destroy_counts.append(action.plan_destroy(source_datasets, target_datasets))

        fs = [e.submit(_plan_table, ctx, d.dataset_id, conf_dir) for d in target_datasets]
        for add_count, change_count, destroy_count in as_completed(fs):
            add_counts.append(add_count)
            change_counts.append(change_count)
            destroy_counts.append(destroy_count)

    if not any(chain.from_iterable([add_counts, change_counts, destroy_counts])):
        echo(msg.MESSAGE_SUMMARY_NO_CHANGE)
        echo()
    else:
        echo(msg.MESSAGE_PLAN_SUMMARY.format(
            sum(add_counts), sum(change_counts), sum(destroy_counts)))
        echo()
        if detailed_exitcode:
            sys.exit(2)


@cli.command(help=msg.HELP_COMMAND_APPLY)
@click.argument('conf-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.option('--mode', '-m', type=click.Choice([
    SchemaMigrationMode.SELECT_INSERT.value,
    SchemaMigrationMode.SELECT_INSERT_BACKUP.value,
    SchemaMigrationMode.REPLACE.value,
    SchemaMigrationMode.REPLACE_BACKUP.value,
    SchemaMigrationMode.DROP_CREATE.value,
    SchemaMigrationMode.DROP_CREATE_BACKUP.value]),
              required=True, default=SchemaMigrationMode.SELECT_INSERT.value,
              help=msg.HELP_OPTION_MIGRATION_MODE)
@click.option('--backup-dataset', '-b', type=str, required=False,
              help=msg.HELP_OPTION_BACKUP_DATASET)
@click.pass_context
def apply(ctx, conf_dir, dataset, exclude_dataset, mode, backup_dataset):
    add_counts, change_counts, destroy_counts = [], [], []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        with DatasetAction(project=ctx.obj['project'],
                           credential_file=ctx.obj['credential_file'],
                           no_color=not ctx.obj['color'],
                           parallelism=ctx.obj['parallelism'],
                           debug=ctx.obj['debug']) as action:
            source_datasets = action.list_datasets(dataset, exclude_dataset)
            target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)

            add_counts.append(action.add(source_datasets, target_datasets))
            change_counts.append(action.change(source_datasets, target_datasets))
            destroy_counts.append(action.destroy(source_datasets, target_datasets))

        fs = [e.submit(_apply_table, ctx, d.dataset_id, conf_dir, mode, backup_dataset)
              for d in target_datasets]
        for add_count, change_count, destroy_count in as_completed(fs):
            add_counts.append(add_count)
            change_counts.append(change_count)
            destroy_counts.append(destroy_count)

    if not any(chain.from_iterable([add_counts, change_counts, destroy_counts])):
        echo(msg.MESSAGE_SUMMARY_NO_CHANGE)
        echo()
    else:
        echo(msg.MESSAGE_APPLY_SUMMARY.format(
            sum(add_counts), sum(change_counts), sum(destroy_counts)))
        echo()


@cli.group(help=msg.HELP_COMMAND_DESTROY)
@click.pass_context
def destroy(ctx):
    pass


@destroy.command('plan', help=msg.HELP_COMMAND_PLAN_DESTROY)
@click.argument('conf-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--detailed-exitcode', is_flag=True, default=False,
              help=msg.HELP_OPTION_DETAILED_EXIT_CODE)
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.pass_context
def plan_destroy(ctx, conf_dir, detailed_exitcode, dataset, exclude_dataset):
    echo(msg.MESSAGE_PLAN_HEADER)

    destroy_counts = []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        with DatasetAction(project=ctx.obj['project'],
                           credential_file=ctx.obj['credential_file'],
                           no_color=not ctx.obj['color'],
                           parallelism=ctx.obj['parallelism'],
                           debug=ctx.obj['debug']) as action:
            source_datasets = action.list_datasets(dataset, exclude_dataset)
            target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)

            destroy_counts.append(action.plan_intersection_destroy(
                source_datasets, target_datasets))

        fs = [e.submit(_plan_destroy_table, ctx, d.dataset_id) for d in target_datasets]
        for r in as_completed(fs):
            destroy_counts.append(r)

    if not any(destroy_counts):
        echo(msg.MESSAGE_SUMMARY_NO_CHANGE)
        echo()
    else:
        echo(msg.MESSAGE_PLAN_DESTROY_SUMMARY.format(sum(destroy_counts)))
        echo()
        if detailed_exitcode:
            sys.exit(2)


@destroy.command('apply', help=msg.HELP_COMMAND_APPLY_DESTROY)
@click.argument('conf-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.pass_context
def apply_destroy(ctx, conf_dir, dataset, exclude_dataset):
    destroy_counts = []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        with DatasetAction(project=ctx.obj['project'],
                           credential_file=ctx.obj['credential_file'],
                           no_color=not ctx.obj['color'],
                           parallelism=ctx.obj['parallelism'],
                           debug=ctx.obj['debug']) as action:
            source_datasets = action.list_datasets(dataset, exclude_dataset)
            target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)

            fs = [e.submit(_apply_destroy_table, ctx, d.dataset_id) for d in target_datasets]
            for r in as_completed(fs):
                destroy_counts.append(r)

            destroy_counts.append(action.intersection_destroy(
                source_datasets, target_datasets))

    if not any(destroy_counts):
        echo(msg.MESSAGE_SUMMARY_NO_CHANGE)
        echo()
    else:
        echo(msg.MESSAGE_APPLY_DESTROY_SUMMARY.format(sum(destroy_counts)))
        echo()


if __name__ == '__main__':
    cli()
