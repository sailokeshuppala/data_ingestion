FROM apache/airflow:2.8.1

USER root

# Install system dependencies: ffmpeg and git
RUN apt-get update && apt-get install -y ffmpeg git

# Switch to airflow user before pip installs
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    git+https://github.com/openai/whisper.git \
    boto3 \
    pandas \
    pyarrow \
    pydantic \
    python-dotenv \
    soundfile \
    tqdm
