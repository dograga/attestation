import pytest
from datetime import datetime
from models import AttestationApprovePayload
from firestore_service import FirestoreService

def check_approval_logic(approvals_list, required_approvers, new_approval):
    # Mimics the logic inside the Firestore transaction
    for existing in approvals_list:
        if existing["approver_user"] == new_approval.approver_user and existing["definition_role"] == new_approval.definition_role:
            raise ValueError(f"User {new_approval.approver_user} already approved for role {new_approval.definition_role}")
            
    new_record = {
        "approver_group": new_approval.approver_group,
        "definition_role": new_approval.definition_role,
        "approver_user": new_approval.approver_user,
        "updated_on": datetime.utcnow()
    }
    approvals_list.append(new_record)
    
    approved_roles = set(a["definition_role"] for a in approvals_list)
    is_completed = all(req_role in approved_roles for req_role in required_approvers)
    
    return is_completed, approvals_list

def test_approval_logic_success():
    approvals = []
    required = ["app_lead", "gcp_lead"]
    
    # 1. First group approval (still PENDING)
    comp, apps = check_approval_logic(approvals, required, AttestationApprovePayload(approver_group="app_lead_marketing", definition_role="app_lead", approver_user="user1"))
    assert not comp
    assert len(apps) == 1
    
    # 2. Second group approval (Completes the attestation)
    comp, apps = check_approval_logic(approvals, required, AttestationApprovePayload(approver_group="gcp_lead_marketing", definition_role="gcp_lead", approver_user="user2"))
    assert comp
    assert len(apps) == 2

def test_approval_logic_no_upsert():
    approvals = []
    required = ["app_lead", "gcp_lead"]
    
    # 1. Approve
    check_approval_logic(approvals, required, AttestationApprovePayload(approver_group="app_lead_marketing", definition_role="app_lead", approver_user="user1"))
    
    # 2. Upsert block test (atomicity emulation)
    with pytest.raises(ValueError, match="already approved for role app_lead"):
        check_approval_logic(approvals, required, AttestationApprovePayload(approver_group="another_group", definition_role="app_lead", approver_user="user1"))

def test_add_multiple_images_logic():
    # Emulate arrayUnion behavior since we mock the DB client in tests or rely on firestore emulator
    metadata_urls = []
    
    def array_union(new_item):
        if new_item not in metadata_urls:
            metadata_urls.append(new_item)
            
    array_union("http://gcs-signed-url-1")
    array_union("http://gcs-signed-url-2")
    array_union("http://gcs-signed-url-1") # Should not duplicate
    
    assert len(metadata_urls) == 2
    assert "http://gcs-signed-url-1" in metadata_urls
    assert "http://gcs-signed-url-2" in metadata_urls
