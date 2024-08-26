import sys
from pathlib import Path
# Set the module paths
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import polars as pl

from api.configuration.config import TAB_DIR


by_stage = pl.read_parquet(f'{TAB_DIR}/by_stage.parquet')

stages_reached = pl.read_parquet(f'{TAB_DIR}/stages_reached.parquet')

print(by_stage.head())