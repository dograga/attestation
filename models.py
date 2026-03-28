from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class AttestationDefinition(BaseModel):
    """Schema for attestation_definitions collection"""
    cycle: str = Field(..., description="monthly | yearly | quarterly | adhoc")
    required_approvers: List[str] = Field(..., description="List of required approver groups")

class AttestationRecord(BaseModel):
    """Schema for individual attestation inside history"""
    attestator_group: str
    attestator_user: str
    updated_on: datetime

class AttestationReferencePayload(BaseModel):
    """Payload to create a new central attestation reference"""
    source_type: str = Field(..., description="Source type matching a definition")
    reference_id: str = Field(..., description="Unique reference ID for this attestation (e.g., EX-9912)")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Static payload for the central reference")

class AttestationTaskPayload(BaseModel):
    """Payload to initiate or update an attestation task for a period"""
    period_key: str = Field(..., description="Period key for the execution (e.g., 2026-03)")
    mandatory_attestators: List[str] = Field(..., description="List of exact explicit groups required for attestation")

class AttestPayload(BaseModel):
    """Payload for submitting an attestation"""
    attestator_group: str
    attestator_user: str
