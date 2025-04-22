import whisper
import tempfile
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

class WhisperTranscriber:
    def __init__(self):
        # Load the Whisper model specified by the environment variable or default to 'base'
        self.model = whisper.load_model(os.getenv("WHISPER_MODEL", "base"))

    def transcribe_audio_stream(self, audio_stream: bytes) -> str:
        try:
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmpfile:
                # Write the audio stream to the temporary file
                tmpfile.write(audio_stream)
                tmpfile.flush()  # Ensure data is written to disk

                # Transcribe the audio using Whisper
                result = self.model.transcribe(tmpfile.name)
                
                # Return the transcription text
                return result["text"]
        
        except Exception as e:
            # Handle any exceptions during the transcription process
            logger.error(f"Error during transcription: {e}")
            raise
