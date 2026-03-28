#!/bin/bash
echo "Starting FastAPI Attestation Service..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
