"""
streaming/checkpoint.py — Checkpoint Path Utilities.

Module   : streaming.checkpoint
Concept  : Structured Streaming checkpoint management
When     : Every streaming query must have a checkpoint location.
           Checkpoints store query progress (offsets, state) so that a
           restarted query resumes exactly where it left off.

What it is
----------
Checkpointing is the mechanism that gives Structured Streaming its fault
tolerance.  Spark writes to the checkpoint directory at the end of each
micro-batch.  On restart, the query reads the checkpoint to determine which
offsets have been committed and picks up from there.

Checkpoint directory layout
---------------------------
<checkpoint_base>/<pipeline_name>/
  commits/          Committed batch IDs (offset log).
  offsets/          Per-batch source offsets read but not yet committed.
  sources/          Source-specific metadata (Delta table versions, etc.).
  state/            Stateful operator state (for aggregations, joins).
  metadata          Stream metadata (run ID, schema, etc.).

Rules
-----
1. Each pipeline must have its own unique checkpoint path.
   Sharing a checkpoint between two queries corrupts both.

2. Change the checkpoint path when you change the schema or source —
   an incompatible checkpoint causes the query to fail on restart.

3. Clear the checkpoint when intentionally resetting a pipeline to re-process
   from the beginning.  Never delete checkpoints while a query is running.

4. On Databricks, use DBFS or Unity Catalog Volumes for checkpoints —
   not the local driver filesystem (lost on cluster restart).

Public API
----------
get_checkpoint_path(config) -> str
    Construct the full checkpoint path.

checkpoint_options(config) -> dict
    Dict of .options() for DataStreamWriter.

clear_checkpoint(path) -> None
    Delete a checkpoint directory (local filesystem only — use dbutils for DBFS).
"""

from __future__ import annotations

from streaming.config import StreamingConfig


def get_checkpoint_path(config: StreamingConfig) -> str:
    """
    Return the full checkpoint directory path for this pipeline.

    Parameters
    ----------
    config : StreamingConfig

    Returns
    -------
    str
        '<checkpoint_base>/<pipeline_name>'
    """
    return config.checkpoint_path


def checkpoint_options(config: StreamingConfig) -> dict:
    """
    Return a dict of options for DataStreamWriter to set the checkpoint location.

    Parameters
    ----------
    config : StreamingConfig

    Returns
    -------
    dict
        {'checkpointLocation': '<checkpoint_path>'}

    Examples
    --------
        opts = checkpoint_options(config)
        df.writeStream.options(**opts).format("delta").start()
    """
    return {"checkpointLocation": config.checkpoint_path}


def clear_checkpoint(path: str) -> None:
    """
    Delete a checkpoint directory from the local filesystem.

    Use only for local development / unit testing.  On Databricks, use
    dbutils.fs.rm(path, recurse=True) to delete DBFS checkpoints.

    Parameters
    ----------
    path : str
        Checkpoint directory to delete.

    Returns
    -------
    None
    """
    import os
    import shutil

    if os.path.exists(path):
        shutil.rmtree(path)


def checkpoint_exists(path: str) -> bool:
    """
    Return True if a checkpoint directory exists and contains commit metadata.

    Parameters
    ----------
    path : str

    Returns
    -------
    bool
    """
    import os

    commits_dir = os.path.join(path, "commits")
    return os.path.isdir(commits_dir)
