from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import numpy as np
import soundfile as sf
import io
import time
from collections import defaultdict
from airflow.exceptions import AirflowSkipException

from src.r2_client import R2Client
from src.transcriber import WhisperTranscriber
from src.tokenizer import tokenise
from src.storage import ParquetStorage
from src.schemas import AudioResult, PipelineSummary

load_dotenv()
logger = logging.getLogger(__name__)
FILE_LIMIT = 2 # can be parameterized via Airflow Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='audio_processing_pipeline_limited',
    default_args=default_args,
    description='Pipeline to process 20 audio files from R2: transcribe, tokenize, store.',
    schedule_interval=None,
    catchup=False,
    tags=['audio', 'transcription', 'tokenization']
)

def list_audio_files_task(**context):
    client = R2Client()
    files = client.list_audio_files()[:FILE_LIMIT]
    if not files:
        logger.warning("No audio files found.")
        raise ValueError("No files available for processing")
    logger.info(f"Found {len(files)} files to process.")
    context['ti'].xcom_push(key='audio_files', value=files)

def process_audio_files_task(**context):
    client = R2Client()
    transcriber = WhisperTranscriber()
    audio_files = context['ti'].xcom_pull(task_ids='list_audio_files', key='audio_files')
    if not audio_files:
        logger.error("No files received for processing")
        raise AirflowSkipException("No files to process")

    results = []

    for idx, file_key in enumerate(audio_files, 1):
        try:
            logger.info(f"Processing file {idx}/{len(audio_files)}: {file_key}")
            audio_stream = client.stream_file(file_key)

            # Transcribe
            try:
                transcription = transcriber.transcribe_audio_stream(audio_stream)
            except Exception as e:
                logger.error(f"Whisper failed for {file_key}: {str(e)}")
                transcription = ""  # fallback to empty transcript

            # Tokenize
            try:
                audio_np, _ = sf.read(io.BytesIO(audio_stream), dtype='int16')
                tokens_tensor = tokenise(audio_np)
                tokens = tokens_tensor.cpu().tolist()
            except Exception as e:
                logger.error(f"Tokenization failed for {file_key}: {str(e)}")
                tokens = []

            results.append({
                "key": file_key,
                "transcription": transcription,
                "token_array": tokens
            })

        except Exception as e:
            logger.error(f"Critical failure in processing {file_key}: {str(e)}")
            results.append({
                "key": file_key,
                "transcription": "",
                "token_array": []
            })

    context['ti'].xcom_push(key='tokenized_data', value=results)

def save_results_task(**context):
    start_time = time.time()
    tokenized_data = context['ti'].xcom_pull(task_ids='process_audio_files', key='tokenized_data')
    if not tokenized_data:
        logger.error("No data received for saving")
        raise AirflowSkipException("No results to save")

    storage = ParquetStorage()
    logger.info("Initialized ParquetStorage to write results.")

    metrics = {
        'total_files': len(tokenized_data),
        'failed_files': [],
        'token_counts': [],
        'token_distribution': defaultdict(int)
    }

    token_bins = [(0, 100), (101, 300), (301, 500), (501, 1000), (1001, float('inf'))]

    for item in tokenized_data:
        try:
            tokens = item['token_array']
            transcription = item['transcription']
            key = item['key']
            metrics['token_counts'].append(len(tokens))

            # Log token count bin
            for bin_min, bin_max in token_bins:
                if bin_min <= len(tokens) <= bin_max:
                    label = f"{bin_min}-{bin_max if bin_max != float('inf') else 'inf'}"
                    metrics['token_distribution'][label] += 1
                    break

            logger.info(f"Saving result for: {key} | Tokens: {len(tokens)} | Transcription length: {len(transcription)}")

            storage.save_result(AudioResult(
                file_key=key,
                transcription=transcription,
                tokens=tokens
            ))
            logger.info(f"✔️ Saved result for: {key}")

        except Exception as e:
            logger.warning(f"❌ Failed to save result for {item['key']}: {str(e)}")
            metrics['failed_files'].append(item['key'])

    summary = PipelineSummary(
        total_files=metrics['total_files'],
        total_tokens=sum(metrics['token_counts']),
        avg_tokens_per_file=(sum(metrics['token_counts']) / metrics['total_files']) if metrics['total_files'] else 0,
        max_tokens_in_file=max(metrics['token_counts'], default=0),
        min_tokens_in_file=min(metrics['token_counts'], default=0),
        files_with_zero_tokens=metrics['token_counts'].count(0),
        token_distribution_bins=dict(metrics['token_distribution']),
        failed_files=metrics['failed_files'],
        processing_time_seconds=round(time.time() - start_time, 2)
    )

    try:
        storage.save_summary(summary)
        logger.info(f"✅ Saved summary. Processing time: {summary.processing_time_seconds}s")
        logger.info(f"📊 Summary stats: {summary.model_dump()}")
    except Exception as e:
        logger.error(f"❌ Failed to save summary: {str(e)}")

    storage.close()
    logger.info("🧹 ParquetStorage closed successfully.")


# Tasks
list_audio_files = PythonOperator(
    task_id='list_audio_files',
    python_callable=list_audio_files_task,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

process_audio_files = PythonOperator(
    task_id='process_audio_files',
    python_callable=process_audio_files_task,
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

save_results = PythonOperator(
    task_id='save_results_to_parquet',
    python_callable=save_results_task,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

# Set task dependencies
list_audio_files >> process_audio_files >> save_results
