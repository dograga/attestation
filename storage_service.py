import structlog
from google.cloud import storage
from datetime import timedelta
from config import get_settings

logger = structlog.get_logger()
settings = get_settings()

class StorageService:
    def __init__(self):
        self.client = None
        self._initialize_client()
        
    def _initialize_client(self):
        try:
            if settings.firestore_project_id:
                self.client = storage.Client(project=settings.firestore_project_id)
            else:
                self.client = storage.Client()
            logger.info("Storage client initialized")
        except Exception as e:
            logger.error("Failed to initialize Storage client", error=str(e))

    async def upload_evidence(self, source_type: str, reference_id: str, period_key: str, filename: str, file_content: bytes) -> str:
        """Uploads file to GCS and returns the signed URL to access it."""
        if not self.client:
            raise Exception("Storage client not initialized")
            
        bucket = self.client.bucket(settings.gcs_bucket_name)
        blob_path = f"{source_type}/{reference_id}/{period_key}/{filename}"
        blob = bucket.blob(blob_path)
        
        blob.upload_from_string(file_content)
        
        url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(days=7),
            method="GET"
        )
        return url

storage_service = StorageService()
