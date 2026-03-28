import structlog
from datetime import datetime
from typing import List, Dict, Any, Optional
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
from dateutil.relativedelta import relativedelta
from config import get_settings
from models import AttestationTaskPayload, AttestPayload, AttestationDefinition, AttestationReferencePayload

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

    async def create_definition(self, source_type: str, definition: AttestationDefinition) -> Dict[str, Any]:
        """Create or update an attestation definition."""
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
            
        def_ref = self.client.collection("attestation_definitions").document(source_type)
        data = {
            "cycle": definition.cycle,
            "required_approvers": definition.required_approvers
        }
        await def_ref.set(data)
        logger.info("Definition created", source_type=source_type)
        return {"status": "success", "source_type": source_type, "data": data}

    async def list_definitions(self) -> List[Dict[str, Any]]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        defs = []
        async for doc in self.client.collection("attestation_definitions").stream():
            defs.append({"source_type": doc.id, **doc.to_dict()})
        return defs

    async def get_definition(self, source_type: str) -> Dict[str, Any]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        def_ref = self.client.collection("attestation_definitions").document(source_type)
        doc = await def_ref.get()
        if not doc.exists:
            raise ValueError(f"Definition for {source_type} not found")
        return doc.to_dict()

    async def list_attestations(self) -> List[Dict[str, Any]]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        atts = []
        async for doc in self.client.collection("central_attestations").stream():
            atts.append({"id": doc.id, **doc.to_dict()})
        return atts

    async def get_attestation(self, source_type: str, reference_id: str) -> Dict[str, Any]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        parent_id = f"{source_type}#{reference_id}"
        parent_ref = self.client.collection("central_attestations").document(parent_id)
        doc = await parent_ref.get()
        if not doc.exists:
            raise ValueError(f"Attestation {parent_id} not found")
        return doc.to_dict()

    async def get_attestation_history(self, source_type: str, reference_id: str, period_key: str) -> Dict[str, Any]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        parent_id = f"{source_type}#{reference_id}"
        history_ref = self.client.collection("central_attestations").document(parent_id).collection("history").document(period_key)
        doc = await history_ref.get()
        if not doc.exists:
            raise ValueError(f"History for {period_key} not found")
        return doc.to_dict()

    async def get_all_histories(self, source_type: str, reference_id: str) -> List[Dict[str, Any]]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        parent_id = f"{source_type}#{reference_id}"
        history_ref = self.client.collection("central_attestations").document(parent_id).collection("history")
        
        histories = []
        async for doc in history_ref.stream():
            histories.append({"period_key": doc.id, **doc.to_dict()})
            
        return histories

    async def create_attestation_reference(self, payload: AttestationReferencePayload) -> Dict[str, Any]:
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        
        def_ref = self.client.collection("attestation_definitions").document(payload.source_type)
        if not (await def_ref.get()).exists:
            raise ValueError(f"Definition for {payload.source_type} does not exist.")
            
        parent_id = f"{payload.source_type}#{payload.reference_id}"
        parent_ref = self.client.collection("central_attestations").document(parent_id)
        
        data = {
            "source_type": payload.source_type,
            "reference_id": payload.reference_id,
            "status": "PENDING",
            "payload": payload.payload
        }
        
        # Native Firestore atomic lock against concurrent creations
        try:
            await parent_ref.create(data)
        except AlreadyExists:
            raise ValueError(f"Attestation reference {payload.reference_id} already exists.")
            
        return {"status": "success", "parent_id": parent_id, "data": data}

    async def create_attestation_task(self, source_type: str, reference_id: str, payload: AttestationTaskPayload) -> Dict[str, Any]:
        """Create a history task period initialized for an attestation."""
        if not self.client:
            raise FirestoreError("Firestore client not initialized")

        parent_id = f"{source_type}#{reference_id}"
        parent_ref = self.client.collection("central_attestations").document(parent_id)
        
        if not (await parent_ref.get()).exists:
            raise ValueError(f"Attestation reference {reference_id} does not exist. Create the reference first.")
            
        period_key = payload.period_key
        history_ref = parent_ref.collection("history").document(period_key)
        
        history_data = {
            "attestation_status": "PENDING",
            "mandatory_attestators": payload.mandatory_attestators,
            "attestations": [],
            "metadata_urls": []
        }
        
        # Native Firestore atomic lock against concurrent timeline assignments
        try:
            await history_ref.create(history_data)
        except AlreadyExists:
             raise ValueError(f"Task for period {period_key} already exists.")

        return {"status": "success", "parent_id": parent_id, "period_key": period_key}

    async def add_metadata_url(self, source_type: str, reference_id: str, period_key: str, url: str):
        if not self.client:
            raise FirestoreError("Firestore client not initialized")
        
        parent_id = f"{source_type}#{reference_id}"
        history_ref = self.client.collection("central_attestations").document(parent_id).collection("history").document(period_key)
        
        history_doc = await history_ref.get()
        if not history_doc.exists:
            raise ValueError("History document does not exist for this period_key")

        await history_ref.update({
            "metadata_urls": firestore.ArrayUnion([url])
        })
        return {"status": "success", "url": url}

    async def attest_task(self, source_type: str, reference_id: str, period_key: str, attestation: AttestPayload):
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
            cycle = definition.get("cycle", "adhoc")

            history_doc = await history_ref.get(transaction=transaction)
            if not history_doc.exists:
                raise ValueError(f"History doc for period {period_key} not found")
            history = history_doc.to_dict()
            mandatory_attestators = history.get("mandatory_attestators", [])
            
            attestations = history.get("attestations", [])
            
            for existing in attestations:
                if existing["attestator_user"] == attestation.attestator_user and existing["attestator_group"] == attestation.attestator_group:
                    raise ValueError(f"User {attestation.attestator_user} already attested for group {attestation.attestator_group}")
            
            new_record = {
                "attestator_group": attestation.attestator_group,
                "attestator_user": attestation.attestator_user,
                "updated_on": datetime.utcnow()
            }
            attestations.append(new_record)
            
            approved_groups = set(a["attestator_group"] for a in attestations)
            is_completed = all(req_group in approved_groups for req_group in mandatory_attestators)
            
            history_update = {"attestations": attestations}
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
