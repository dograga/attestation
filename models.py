from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class AttestationDefinition(BaseModel):
    """Schema for attestation_definitions collection"""
    cycle: str = Field(..., description="monthly | yearly | quarterly | adhoc")
    required_approvers: List[str] = Field(..., description="List of required approver groups")

class Approval(BaseModel):
    """Schema for individual approval inside history"""
    approver_group: str
    approver_user: str
    updated_on: datetime

class AttestationSubmitPayload(BaseModel):
    """Payload to initiate or update an attestation submission"""
    reference_id: str = Field(..., description="Reference ID for the attestation (e.g., EX-9912)")
    period_key: str = Field(..., description="Period key for the execution (e.g., 2026)")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Dynamic payload for specific data")

class AttestationApprovePayload(BaseModel):
    """Payload for submitting an approval"""
    approver_group: str
    approver_user: str
