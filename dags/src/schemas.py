from pydantic import BaseModel
from typing import List, Dict

class AudioResult(BaseModel):
    file_key: str
    transcription: str
    tokens: List[int]

class PipelineSummary(BaseModel):
    total_files: int
    total_tokens: int
    avg_tokens_per_file: float
 
    max_tokens_in_file: int
    min_tokens_in_file: int
    files_with_zero_tokens: int
    token_distribution_bins: Dict[str, int]  # e.g., "0-100": 10, "101-300": 20, ...
    failed_files: List[str]
    processing_time_seconds: float