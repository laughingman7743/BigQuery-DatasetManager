# -*- coding: utf-8 -*-
from __future__ import absolute_import

import yaml
from past.types import unicode

from bqdm.model.dataset import BigQueryAccessEntry, BigQueryDataset
from bqdm.model.schema import BigQuerySchemaField
from bqdm.model.table import BigQueryTable
from bqdm.util import str_representer

yaml.add_representer(str, str_representer)
yaml.add_representer(unicode, str_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessEntry, BigQueryAccessEntry.represent)
yaml.add_representer(BigQueryTable, BigQueryTable.represent)
yaml.add_representer(BigQuerySchemaField, BigQuerySchemaField.represent)
