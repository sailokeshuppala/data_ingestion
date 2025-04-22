
# Audio Processing Pipeline

## Overview

This is an **audio processing pipeline** designed for efficient handling of audio files, from transcription to tokenization and storage. The pipeline supports reading audio files from a remote object storage (Cloudflare R2), transcribing them using **Whisper**, tokenizing the audio content, and saving the results in a **Parquet** format for downstream analysis.

The pipeline is orchestrated using **Apache Airflow**, allowing for easy scheduling, monitoring, and logging of tasks. The entire flow is designed to be scalable, robust, and efficient, with an emphasis on batch processing and parallelism.

---

## Key Features

- **Parallelized Execution**: All steps (list files, transcribe, tokenize, store) run in parallel, leveraging Airflow's concurrency and task dependencies.
- **Batch Processing**: Process audio files in batches to optimize performance, ensuring the pipeline handles large datasets efficiently.
- **Error Handling & Logging**: In-depth logging for each stage of the pipeline, with retries and detailed error reports for transparency and debugging.
- **Scalable Storage**: Uses **Cloudflare R2** for storage and **Parquet** for structured data storage, enabling easy scalability and querying of results.
- **Tokenization and Transcription**: Utilizes **Whisper** for transcription and custom tokenization for further NLP tasks or model training.
  
---

## Requirements

- Python 3.8+
- Apache Airflow 2.x
- Cloudflare R2 (or any object storage with S3-compatible APIs)
- Parquet (for storage)
- Required Python libraries:
  - `boto3` (for R2 integration)
  - `soundfile` (for audio processing)
  - `numpy` (for numerical operations)
  - `whisper` (for transcription)
  - `dotenv` (for environment variable management)
  - `pandas` (for data handling and output)
  - `airflow` (for orchestration)

---

## Setup & Installation

### Clone the Repository

```bash
git clone https://github.com/your-repo/audio-processing-pipeline.git
cd audio-processing-pipeline
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Set Up Environment Variables

Create a `.env` file to store your Cloudflare R2 access credentials and other necessary configurations.

```bash
R2_ACCESS_KEY=your_r2_access_key
R2_SECRET_KEY=your_r2_secret_key
R2_ENDPOINT=https://your_r2_endpoint
FERNET_KEY=your_fernet_key
```

---

## Airflow Setup

1. **Docker Setup (Optional but recommended)**

   The pipeline includes a `docker-compose.yml` file to set up all necessary services for Airflow.

   ```bash
   docker-compose up
   ```

   This will set up:
   - Postgres (Airflow metadata database)
   - Airflow Webserver, Scheduler, and Worker
   - Dockerized environment for easy orchestration.

2. **Airflow DAG**

   Once your environment is set up, the DAG (`audio_processing_pipeline_limited`) can be triggered from the Airflow UI or programmatically. This DAG handles the entire workflow:
   - **List Audio Files**
   - **Transcribe Audio Files**
   - **Tokenize Audio**
   - **Save Results to Parquet**

---

## Pipeline Workflow

The pipeline consists of three main tasks, each executed in sequence:

### 1. **List Audio Files**

   This task interacts with Cloudflare R2 to list audio files available for processing. It retrieves the first batch of files (limited to `FILE_LIMIT`), which are then passed on for transcription.

### 2. **Transcribe Audio Files**

   In this task, the audio files are streamed and processed using the **Whisper** model to generate transcriptions. If transcription fails, it falls back to an empty transcript, but detailed logs are created for troubleshooting.

### 3. **Tokenize Audio**

   The audio files are tokenized after transcription using a custom tokenization process. This process converts the audio into a structured token representation for further use, such as training machine learning models.

### 4. **Store Results**

   The results (transcriptions and tokens) are stored as Parquet files, ensuring optimized and efficient storage. The pipeline also generates a summary of the processing metrics (e.g., total tokens, token distribution, processing time) which is stored alongside the Parquet data.

---

## Logs & Monitoring

The pipeline leverages **Airflow's logging capabilities** to provide detailed logs at every step of the process, including error handling and retries. Logs are visible on the Airflow UI and are also saved in `./logs` for local tracking.

---

## Example Output

After the pipeline finishes processing, the output is stored in **Parquet format**, and you can use tools like `pandas` to easily inspect and query the results.

```python
import pandas as pd

df = pd.read_parquet('path_to_output_file.parquet')
print(df)
```

This will print the processed summary data, including total tokens, processing time, and file-level statistics.

---

## Running the Pipeline

The DAG can be triggered either from the Airflow UI or by running the following command:

```bash
airflow dags trigger audio_processing_pipeline_limited
```

You can also set it to run on a schedule by modifying the `schedule_interval` in the `DAG` configuration.

---

## Contributing

Feel free to fork this project and contribute! Open a pull request for any improvements, bug fixes, or feature additions.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Contact

For any questions or inquiries, please contact [your-email@example.com](mailto:your-email@example.com).

---

### 🛠️ Built with ❤️ by [Your Name/Your Team]
