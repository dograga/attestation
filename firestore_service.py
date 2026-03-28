import structlog
from datetime import datetime
from typing import List, Dict, Any, Optional
from google.cloud import firestore
from dateutil.relativedelta import relativedelta
from config import get_settings
from models import AttestationSubmitPayload, AttestationApprovePayload

logger = structlog.get_logger()
settings = get_settings()

class FirestoreError(Exception):
    pass

class FirestoreService:
    def __init__(self):
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        try:
            project_id = settings.firestore_project_id
            db_name = settings.firestore_db_name
            if project_id:
                self.client = firestore.AsyncClient(project=project_id, database=db_name)
            else:
                self.client = firestore.AsyncClient(database=db_name)
            logger.info("Firestore async client initialized")
        except Exception as e:
            logger.error("Failed to initialize Firestore client", error=str(e))
            self.client = None

    async def submit_attestation(self, source_type: str, payload: AttestationSubmitPayload) -> Dict[str, Any]:
        """Initiate or update a period_key with a dynamic payload."""
        if not self.client:
            raise FirestoreError("Firestore client not initialized")

        parent_id = f"{source_type}#{payload.reference_id}"
        period_key = payload.period_key

        def_ref = self.client.collection("attestation_definitions").document(source_type)
        def_doc = await def_ref.get()
        if not def_doc.exists:
            raise ValueError(f"Definition for {source_type} does not exist.")

        parent_ref = self.client.collection("central_attestations").document(parent_id)
        history_ref = parent_ref.collection("history").document(period_key)

        batch = self.client.batch()

        # Update or create the parent
        parent_data = {
            "source_type": source_type,
            "reference_id": payload.reference_id,
        }
        
        # We also want to guarantee status is set if this is new
        parent_doc = await parent_ref.get()
        if not parent_doc.exists:
             parent_data["status"] = "PENDING"
        
        batch.set(parent_ref, parent_data, merge=True)

        history_doc = await history_ref.get()
        history_data = {
            "payload": payload.payload,
        }
        
        if not history_doc.exists:
            history_data["attestation_status"] = "PENDING"
            history_data["approvals"] = []
            history_data["metadata_urls"] = []
            
        batch.set(history_ref, history_data, merge=True)
        await batch.commit()

        return {"status": "success", "parent_id": parent_id, "period_key": period_key}

    async def add_metadata_url(self, source_type: str, reference_id: str, period_key: str, url: str):
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        
        parent_id = f"{source_type}#{reference_id}"
        history_ref = self.client.collection("central_attestations").document(parent_id).collection("history").document(period_key)
        
        # Check if exists
        history_doc = await history_ref.get()
        if not history_doc.exists:
            raise ValueError("History document does not exist for this period_key")

        await history_ref.update({
            "metadata_urls": firestore.ArrayUnion([url])
        })
        return {"status": "success", "url": url}

    async def approve_attestation(self, source_type: str, reference_id: str, period_key: str, approval: AttestationApprovePayload):
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
            
        parent_id = f"{source_type}#{reference_id}"
        def_ref = self.client.collection("attestation_definitions").document(source_type)
        parent_ref = self.client.collection("central_attestations").document(parent_id)
        history_ref = parent_ref.collection("history").document(period_key)
        
        transaction = self.client.transaction()
        
        @firestore.async_transactional
        async def update_in_transaction(transaction, def_ref, parent_ref, history_ref):
            def_doc = await def_ref.get(transaction=transaction)
            if not def_doc.exists:
                raise ValueError(f"Definition for {source_type} not found")
            definition = def_doc.to_dict()
            required_approvers = definition.get("required_approvers", [])
            cycle = definition.get("cycle", "adhoc")

            history_doc = await history_ref.get(transaction=transaction)
            if not history_doc.exists:
                raise ValueError(f"History doc for period {period_key} not found")
            history = history_doc.to_dict()
            
            approvals = history.get("approvals", [])
            
            # Prevent upsert / enforce atomicity
            for existing_approval in approvals:
                if existing_approval["approver_user"] == approval.approver_user and existing_approval["approver_group"] == approval.approver_group:
                    raise ValueError(f"User {approval.approver_user} already approved for group {approval.approver_group}")
            
            new_approval = {
                "approver_group": approval.approver_group,
                "approver_user": approval.approver_user,
                "updated_on": datetime.utcnow()
            }
            approvals.append(new_approval)
            
            approved_groups = set(a["approver_group"] for a in approvals)
            is_completed = all(req_group in approved_groups for req_group in required_approvers)
            
            history_update = {"approvals": approvals}
            if is_completed:
                history_update["attestation_status"] = "COMPLETED"
                
            transaction.update(history_ref, history_update)
            
            if is_completed:
                last_attested = datetime.utcnow()
                next_cycle_due = last_attested
                if cycle == "monthly":
                    next_cycle_due = last_attested + relativedelta(months=1)
                elif cycle == "yearly":
                    next_cycle_due = last_attested + relativedelta(years=1)
                elif cycle == "quarterly":
                    next_cycle_due = last_attested + relativedelta(months=3)
                    
                transaction.update(parent_ref, {
                    "status": "COMPLETED",
                    "last_attested": last_attested,
                    "next_cycle_due": next_cycle_due
                })
                
            return {"status": "success", "is_completed": is_completed}

        return await update_in_transaction(transaction, def_ref, parent_ref, history_ref)

firestore_service = FirestoreService()
