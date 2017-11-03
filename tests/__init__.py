# -*- coding: utf-8 -*-
import yaml

from bqdm.util import str_representer
from bqdm.model import BigQueryDataset, BigQueryAccessEntry


yaml.add_representer(str, str_representer)
yaml.add_representer(BigQueryDataset, BigQueryDataset.represent)
yaml.add_representer(BigQueryAccessEntry, BigQueryAccessEntry.represent)
