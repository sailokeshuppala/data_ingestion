from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from dotenv import load_dotenv
import logging
import requests
import time
from typing import List, Dict
from collections import defaultdict
import glob
import io
import soundfile as sf
import pandas as pd
from src.r2_client import R2Client
from src.schemas import AudioResult, PipelineSummary
from src.storage import ParquetStorage
from src.tokenizer import tokenise

load_dotenv()

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG Definition
dag = DAG(
    dag_id='audio_processing_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['audio', 'transcription', 'pipeline'],
    max_active_tasks=32,
    max_active_runs=1,
    params={
        "start_index": 0,
        "max_files": 2000,
        "batch_size": 100
    }
)

# Queues
BATCH_QUEUE = 'batch_queue'
PROCESS_QUEUE = 'process_queue'
SAVE_QUEUE = 'save_queue'

# Whisper Server URL
WHISPER_SERVER_URL = 'http://127.0.0.1:5000/transcribe_batch'

@task(queue=BATCH_QUEUE)
def list_files_with_context() -> List[str]:
    from airflow.operators.python import get_current_context
    context = get_current_context()
    start_index = context['params'].get('start_index', 0)
    max_files = context['params'].get('max_files', 2000)

    client = R2Client()
    all_files = client.list_audio_files()
    selected_files = all_files[start_index:start_index + max_files]
    return selected_files

@task(queue=BATCH_QUEUE)
def batch_files(file_keys: List[str]) -> List[List[str]]:
    from airflow.operators.python import get_current_context
    context = get_current_context()
    batch_size = context['params'].get('batch_size', 100)
    return [file_keys[i:i + batch_size] for i in range(0, len(file_keys), batch_size)]

@task(queue=PROCESS_QUEUE)
def process_batch(batch: List[str]) -> List[Dict]:
    client = R2Client()
    results = []
    
    audio_streams = []
    file_keys = []
    
    # Gather audio files
    for file_key in batch:
        try:
            logger.info(f" Preparing: {file_key}")
            audio_stream = client.stream_file(file_key)
            if not audio_stream:
                logger.warning(f"Empty audio stream: {file_key}")
                continue
            audio_streams.append(audio_stream)
            file_keys.append(file_key)
        except Exception as e:
            logger.error(f"Error fetching file {file_key}: {e}")
            continue

    # Process batch if we have files
    if audio_streams:
        # Get batch response from server
        batch_response = transcribe_audio_with_whisper_batch(audio_streams)
        
        # Process each result
        for file_key, audio_stream, transcription in zip(file_keys, audio_streams, batch_response):
            try:
                # Tokenization
                audio_np, _ = sf.read(io.BytesIO(audio_stream), dtype='int16')
                tokens_tensor = tokenise(audio_np)
                tokens = tokens_tensor.cpu().tolist()
                
                results.append({
                    "key": file_key,
                    "transcription": transcription,
                    "token_array": tokens
                })
            except Exception as e:
                logger.error(f"Processing failed for {file_key}: {e}")
                results.append({
                    "key": file_key,
                    "transcription": "",
                    "token_array": [],
                    "error": str(e)
                })
    
    return results


def transcribe_audio_with_whisper_batch(audio_streams: List[bytes]) -> List[str]:
    """Send multiple audio files to the Whisper server for batch transcription"""
    try:
        # Prepare files for batch request
        files = [("audio", (f"audio_{i}.wav", io.BytesIO(audio_stream), "audio/wav")) 
                for i, audio_stream in enumerate(audio_streams)]
        
        # Send batch request
        response = requests.post(WHISPER_SERVER_URL, files=files)
        
        if response.status_code == 200:
            results = response.json().get("results", [])
            # Ensure we maintain order matching input files
            transcriptions = [r.get("transcription", "") for r in results]
            logger.info(f"Successfully transcribed batch of {len(audio_streams)} files")
            return transcriptions
        else:
            logger.error(f"Batch transcription failed: {response.status_code}, {response.text}")
            return [""] * len(audio_streams)
    except Exception as e:
        logger.error(f"Batch transcription error: {e}")
        return [""] * len(audio_streams)

@task(queue=BATCH_QUEUE)
def check_whisper_health():
    """Check if Whisper server is healthy before processing"""
    try:
        response = requests.get(f"{WHISPER_SERVER_URL.replace('/transcribe_batch', '/health')}", timeout=10)
        if response.status_code != 200:
            raise Exception(f"Health check failed: {response.text}")
        logger.info(" Whisper server is healthy")
        return True
    except Exception as e:
        logger.error(f" Whisper server health check failed: {e}")
        raise
    
@task(queue=SAVE_QUEUE)
def save_batch_results(results: List[Dict]):
    start_time = time.time()
    storage = ParquetStorage()
    token_counts = []
    failed_files = []
    token_distribution = defaultdict(int)

    for item in results:
        try:
            tokens = item['token_array']
            token_counts.append(len(tokens))

            # Binning tokens
            for low, high in [(0, 100), (101, 300), (301, 500), (501, 1000), (1001, float('inf'))]:
                if low <= len(tokens) <= high:
                    label = f"{low}-{int(high) if high != float('inf') else 'inf'}"
                    token_distribution[label] += 1
                    break

            storage.save_result(AudioResult(
                file_key=item['key'],
                transcription=item['transcription'],
                tokens=tokens
            ))
        except Exception as e:
            logger.error(f"Saving failed for {item['key']}: {e}")
            failed_files.append(item['key'])

    summary = PipelineSummary(
        total_files=len(results),
        total_tokens=sum(token_counts),
        avg_tokens_per_file=(sum(token_counts) / len(results)) if results else 0,
        max_tokens_in_file=max(token_counts, default=0),
        min_tokens_in_file=min(token_counts, default=0),
        files_with_zero_tokens=token_counts.count(0),
        token_distribution_bins=dict(token_distribution),
        failed_files=failed_files,
        processing_time_seconds=round(time.time() - start_time, 2)
    )

    storage.save_summary(summary)
    storage.close()
    logger.info(f" Saved summary: {summary.model_dump()}")

@task(queue=SAVE_QUEUE)
def consolidate_parquet_files():
    result_files = glob.glob("shared_storage/results/*.parquet")
    summary_files = glob.glob("shared_storage/summary/*.parquet")

    if result_files:
        df_results = pd.concat([pd.read_parquet(f) for f in result_files], ignore_index=True)
        df_results.to_parquet("shared_storage/results/result.parquet", index=False)
        logger.info(" Combined result parquet saved.")
    else:
        logger.warning(" No result parquet files to consolidate.")

    if summary_files:
        df_summaries = pd.concat([pd.read_parquet(f) for f in summary_files], ignore_index=True)
        df_summaries.to_parquet("shared_storage/summary/summary.parquet", index=False)
        logger.info(" Combined summary parquet saved.")
    else:
        logger.warning(" No summary parquet files to consolidate.")

# DAG Flow
with dag:
    health_check = check_whisper_health()
    file_keys = list_files_with_context()
    file_batches = batch_files(file_keys)
    
    # Ensure health check completes first
    file_keys = health_check >> file_keys
    
    processed_batches = process_batch.expand(batch=file_batches)
    saved = save_batch_results.expand(results=processed_batches)
    final = consolidate_parquet_files()

    chain(saved, final)
