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
        action = DatasetAction(e, project=ctx.obj['project'],
                               credential_file=ctx.obj['credential_file'],
                               no_color=not ctx.obj['color'],
                               debug=ctx.obj['debug'])
        datasets = as_completed(action.export(output_dir, dataset, exclude_dataset))

        fs = []
        for d in datasets:
            action = TableAction(e, d.dataset_id,
                                 project=ctx.obj['project'],
                                 credential_file=ctx.obj['credential_file'],
                                 no_color=not ctx.obj['color'],
                                 debug=ctx.obj['debug'])
            fs.extend(action.export(output_dir))
        as_completed(fs)


@cli.command(help=msg.HELP_COMMAND_PLAN)
@click.argument('conf-dir', type=click.Path(exists=True, dir_okay=True), required=False,
                default='.')
@click.option('--detailed-exitcode', is_flag=True, default=False,
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
        dataset_action = DatasetAction(e, project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
        source_datasets = [d for d in as_completed(dataset_action.list_datasets(
            dataset, exclude_dataset)) if d]
        target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)
        echo('------------------------------------------------------------------------')
        echo()

        add_counts.append(dataset_action.plan_add(source_datasets, target_datasets))
        change_counts.append(dataset_action.plan_change(source_datasets, target_datasets))
        destroy_counts.append(dataset_action.plan_destroy(source_datasets, target_datasets))

        for d in target_datasets:
            target_tables = list_local_tables(conf_dir, d.dataset_id)
            if target_tables is None:
                continue
            table_action = TableAction(e, d.dataset_id,
                                       project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
            source_tables = [t for t in as_completed(table_action.list_tables()) if t]
            if target_tables or source_tables:
                echo('------------------------------------------------------------------------')
                echo()
                add_counts.append(table_action.plan_add(source_tables, target_tables))
                change_counts.append(table_action.plan_change(source_tables, target_tables))
                destroy_counts.append(table_action.plan_destroy(source_tables, target_tables))

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
@click.option('--auto-approve', is_flag=True, default=False,
              help=msg.HELP_OPTION_AUTO_APPROVE)
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
def apply(ctx, conf_dir, auto_approve, dataset, exclude_dataset, mode, backup_dataset):
    # TODO Impl auto-approve option
    add_counts, change_counts, destroy_counts = [], [], []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        dataset_action = DatasetAction(e, project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
        source_datasets = [d for d in as_completed(dataset_action.list_datasets(
            dataset, exclude_dataset)) if d]
        target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)
        echo('------------------------------------------------------------------------')
        echo()

        fs = []
        add_count, add_fs = dataset_action.add(source_datasets, target_datasets)
        add_counts.append(add_count)
        fs.extend(add_fs)
        change_count, change_fs = dataset_action.change(source_datasets, target_datasets)
        change_counts.append(change_count)
        fs.extend(change_fs)
        destroy_count, destroy_fs = dataset_action.destroy(source_datasets, target_datasets)
        destroy_counts.append(destroy_count)
        fs.extend(destroy_fs)
        as_completed(fs)

        fs = []
        for d in target_datasets:
            target_tables = list_local_tables(conf_dir, d.dataset_id)
            if target_tables is None:
                continue
            table_action = TableAction(e, d.dataset_id,
                                       migration_mode=mode,
                                       backup_dataset_id=backup_dataset,
                                       project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
            source_tables = [t for t in as_completed(table_action.list_tables()) if t]
            if target_tables or source_tables:
                echo('------------------------------------------------------------------------')
                echo()
                add_count, add_fs = table_action.add(source_tables, target_tables)
                add_counts.append(add_count)
                fs.extend(add_fs)
                change_count, change_fs = table_action.change(source_tables, target_tables)
                change_counts.append(change_count)
                fs.extend(change_fs)
                destroy_count, destroy_fs = table_action.destroy(source_tables, target_tables)
                destroy_counts.append(destroy_count)
                fs.extend(destroy_fs)
        as_completed(fs)

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
        dataset_action = DatasetAction(e, project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
        source_datasets = [d for d in as_completed(dataset_action.list_datasets(
            dataset, exclude_dataset)) if d]
        target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)
        echo('------------------------------------------------------------------------')
        echo()

        destroy_counts.append(dataset_action.plan_intersection_destroy(
            source_datasets, target_datasets))

        for d in target_datasets:
            table_action = TableAction(e, d.dataset_id,
                                       project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
            source_tables = [t for t in as_completed(table_action.list_tables()) if t]
            if source_tables:
                echo('------------------------------------------------------------------------')
                echo()
                destroy_counts.append(table_action.plan_destroy(source_tables, []))

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
@click.option('--auto-approve', is_flag=True, default=False,
              help=msg.HELP_OPTION_AUTO_APPROVE)
@click.option('--dataset', '-d', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_DATASET)
@click.option('--exclude-dataset', '-e', type=str, required=False, multiple=True,
              help=msg.HELP_OPTION_EXCLUDE_DATASET)
@click.pass_context
def apply_destroy(ctx, conf_dir, auto_approve, dataset, exclude_dataset):
    # TODO Impl auto-approve option
    destroy_counts = []
    with ThreadPoolExecutor(max_workers=ctx.obj['parallelism']) as e:
        dataset_action = DatasetAction(e, project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
        source_datasets = [d for d in as_completed(dataset_action.list_datasets(
            dataset, exclude_dataset)) if d]
        target_datasets = list_local_datasets(conf_dir, dataset, exclude_dataset)
        echo('------------------------------------------------------------------------')
        echo()

        fs = []
        for d in target_datasets:
            table_action = TableAction(e, d.dataset_id,
                                       project=ctx.obj['project'],
                                       credential_file=ctx.obj['credential_file'],
                                       no_color=not ctx.obj['color'],
                                       debug=ctx.obj['debug'])
            source_tables = [t for t in as_completed(table_action.list_tables()) if t]
            if source_tables:
                echo('------------------------------------------------------------------------')
                echo()
                destroy_count, destroy_fs = table_action.destroy(source_tables, [])
                destroy_counts.append(destroy_count)
                fs.extend(destroy_fs)
        as_completed(fs)

        fs = []
        destroy_count, destroy_fs = dataset_action.intersection_destroy(
            source_datasets, target_datasets)
        destroy_counts.append(destroy_count)
        fs.extend(destroy_fs)
        as_completed(fs)

    if not any(destroy_counts):
        echo(msg.MESSAGE_SUMMARY_NO_CHANGE)
        echo()
    else:
        echo(msg.MESSAGE_APPLY_DESTROY_SUMMARY.format(sum(destroy_counts)))
        echo()


if __name__ == '__main__':
    cli()
