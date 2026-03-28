import pytest
from datetime import datetime
from models import AttestPayload

def check_attestation_logic(attestations_list, mandatory_attestators, new_attestation):
    for existing in attestations_list:
        if existing["attestator_user"] == new_attestation.attestator_user and existing["attestator_group"] == new_attestation.attestator_group:
            raise ValueError(f"User {new_attestation.attestator_user} already attested for group {new_attestation.attestator_group}")
            
    new_record = {
        "attestator_group": new_attestation.attestator_group,
        "attestator_user": new_attestation.attestator_user,
        "updated_on": datetime.utcnow()
    }
    attestations_list.append(new_record)
    
    approved_groups = set(a["attestator_group"] for a in attestations_list)
    is_completed = all(req_group in approved_groups for req_group in mandatory_attestators)
    
    return is_completed, attestations_list

def test_attestation_logic_success():
    attestations = []
    required = ["app_lead_marketing", "gcp_lead_infrastructure"]
    
    comp, apps = check_attestation_logic(attestations, required, AttestPayload(attestator_group="app_lead_marketing", attestator_user="user1"))
    assert not comp
    assert len(apps) == 1
    
    comp, apps = check_attestation_logic(attestations, required, AttestPayload(attestator_group="gcp_lead_infrastructure", attestator_user="user2"))
    assert comp
    assert len(apps) == 2

def test_attestation_logic_no_upsert():
    attestations = []
    required = ["app_lead_marketing", "gcp_lead_infrastructure"]
    
    check_attestation_logic(attestations, required, AttestPayload(attestator_group="app_lead_marketing", attestator_user="user1"))
    
    with pytest.raises(ValueError, match="already attested for group"):
        check_attestation_logic(attestations, required, AttestPayload(attestator_group="app_lead_marketing", attestator_user="user1"))
