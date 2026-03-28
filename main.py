"""
FastAPI Attestation Service

Service for managing multi-tenant, periodic Attestations API with Google Firestore backend.
"""

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog
import traceback
from datetime import datetime

from config import get_settings
from models import AttestationSubmitPayload, AttestationApprovePayload, AttestationDefinition
from firestore_service import firestore_service, FirestoreError
from storage_service import storage_service

settings = get_settings()
logger = structlog.get_logger()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error("Unexpected Error", error=str(exc), path=request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": str(exc) if settings.debug else "An unexpected error occurred"}
    )

@app.get("/api/v1/definitions/{source_type}", tags=["Definitions"])
async def get_definition(source_type: str):
    """Fetch an Attestation Definition."""
    try:
        result = await firestore_service.get_definition(source_type)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch definition", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/definitions/{source_type}", tags=["Definitions"])
async def create_definition(source_type: str, payload: AttestationDefinition):
    """
    Create or update an Attestation Definition schema required for processing an attestation.
    """
    try:
        result = await firestore_service.create_definition(source_type, payload)
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error("Definition creation failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/attestations/{source_type}", tags=["Attestations"])
async def submit_attestation(source_type: str, payload: AttestationSubmitPayload):
    """
    Initiate or update a period_key with a dynamic payload.
    """
    try:
        result = await firestore_service.submit_attestation(source_type, payload)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Submission failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/attestations/{source_type}/{reference_id}", tags=["Attestations"])
async def get_attestation(source_type: str, reference_id: str):
    """Fetch the central ledger / parent document for an attestation."""
    try:
        result = await firestore_service.get_attestation(source_type, reference_id)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch attestation", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/attestations/{source_type}/{reference_id}/{period_key}/evidence", tags=["Evidence"])
async def upload_evidence(source_type: str, reference_id: str, period_key: str, file: UploadFile = File(...)):
    """
    Upload image to GCS and append the URL to metadata_urls.
    """
    try:
        content = await file.read()
        url = await storage_service.upload_evidence(source_type, reference_id, period_key, file.filename, content)
        
        await firestore_service.add_metadata_url(source_type, reference_id, period_key, url)
        
        return {"status": "success", "message": "Evidence uploaded successfully", "url": url}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Evidence upload failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/attestations/{source_type}/{reference_id}/history", tags=["History"])
async def get_all_histories(source_type: str, reference_id: str):
    """Fetch all history tracking records for an attestation reference."""
    try:
        result = await firestore_service.get_all_histories(source_type, reference_id)
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error("Failed to fetch all histories", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/attestations/{source_type}/{reference_id}/history/{period_key}", tags=["History"])
async def get_attestation_history(source_type: str, reference_id: str, period_key: str):
    """Fetch the execution history period of an attestation."""
    try:
        result = await firestore_service.get_attestation_history(source_type, reference_id, period_key)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch history", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/attestations/{source_type}/{reference_id}/{period_key}/approve", tags=["Approvals"])
async def approve_attestation(source_type: str, reference_id: str, period_key: str, payload: AttestationApprovePayload):
    """
    Approve an attestation. Update approvals array atomically and check for completion.
    """
    try:
        result = await firestore_service.approve_attestation(source_type, reference_id, period_key, payload)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Approval failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health", tags=["General"])
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
