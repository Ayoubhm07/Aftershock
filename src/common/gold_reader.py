from __future__ import annotations

import io

import pandas as pd
import pyarrow.parquet as pq

from src.common.config import GOLD_ROOT
from src.common.hdfs import WebHdfs


class GoldTableMissing(RuntimeError):
    pass


def available_tables(fs: WebHdfs | None = None) -> list[str]:
    client = fs or WebHdfs()
    return sorted(name for name in client.listdir(GOLD_ROOT))


def load(table: str, fs: WebHdfs | None = None) -> pd.DataFrame:
    client = fs or WebHdfs()
    directory = f"{GOLD_ROOT}/{table}"

    entries = client.listdir(directory)
    parquet_files = [name for name in entries if name.endswith(".parquet")]

    if not parquet_files:
        raise GoldTableMissing(
            f"Aucun fichier Parquet sous {directory}. "
            f"Le DAG gold_insights a-t-il tourne ?")

    frames = [pq.read_table(io.BytesIO(client.read(f"{directory}/{name}"))).to_pandas()
              for name in sorted(parquet_files)]
    return pd.concat(frames, ignore_index=True)
