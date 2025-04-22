import os
from uuid import uuid4
from typing import List
import pandas as pd
import pyarrow.parquet as pq
from src.schemas import AudioResult, PipelineSummary
from venv import logger  # assuming you have a logger set up

class ParquetStorage:
    def __init__(self, batch_size: int = 1000):
        # Updated default paths to mounted shared directory
        self.output_dir = os.getenv("OUTPUT_DIR", "/opt/airflow/shared_storage/results")
        self.summary_dir = os.getenv("SUMMARY_DIR", "/opt/airflow/shared_storage/summary")
        self.batch_size = batch_size

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.summary_dir, exist_ok=True)

        self.results_batch: List[dict] = []

    def save_result(self, result: AudioResult):
        """Save individual result and flush in batches."""
        self.results_batch.append({
            "file_key": result.file_key,
            "transcription": result.transcription,
            "tokens": result.tokens
        })

        if len(self.results_batch) >= self.batch_size:
            self._flush_results_batch()

    def _flush_results_batch(self):
        if not self.results_batch:
            return
        
        df = pd.DataFrame(self.results_batch)
        filename = f"{uuid4().hex}.parquet"
        output_path = os.path.join(self.output_dir, filename)
        df.to_parquet(output_path, engine='pyarrow')
        logger.info(f"Saved batch to {output_path}")
        self.results_batch = []

    def save_summary(self, summary: PipelineSummary):
        df = pd.DataFrame([summary.model_dump()])
        filename = f"{uuid4().hex}_summary.parquet"
        summary_path = os.path.join(self.summary_dir, filename)
        df.to_parquet(summary_path, engine='pyarrow')
        logger.info(f"Saved summary to {summary_path}")

    def close(self):
        """Ensure any remaining results are flushed."""
        self._flush_results_batch()
