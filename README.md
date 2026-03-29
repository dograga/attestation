# FastAPI Attestation Service

A Python FastAPI service with a Google Cloud Firestore backend to manage multi-tenant, periodic Attestations.
The system supports dynamic payloads, multi-party approvals, and a centralized list of image evidence stored securely in Google Cloud Storage (GCS).

## Architecture & Schema (Governance-to-Execution Model)

```mermaid
erDiagram
    attestation_definitions ||--o{ central_attestations : defines
    central_attestations ||--o{ history : execution_periods

    attestation_definitions {
        string doc_id "source_type e.g., security-exemptions"
        string cycle "monthly | yearly | quarterly | adhoc"
        list required_approvers "e.g., ['app_lead', 'gcp_lead']"
    }

    central_attestations {
        string doc_id "source_type#reference_id"
        string source_type
        string reference_id
        string status "PENDING | COMPLETED"
        map payload "Dynamic Dict for use-case"
        timestamp last_attested
        timestamp next_cycle_due
    }

    history {
        string doc_id "period_key e.g., 2026"
        string attestation_status "PENDING | COMPLETED"
        list mandatory_attestators "e.g., exact groups snapshotted at task creation"
        list metadata_urls "GCS Signed URLs"
        list attestations "JSON list of {attestator_group, attestator_user, updated_on}"
    }
```

## Engineering Constraints & Safeguards Implemented

### 1. Atomicity & Concurrency Control
- **Firestore Transactions:** The multi-party attestation logic validates the incoming attestation payload in a strictly isolated **Firestore Transaction**.
- **No-Upsert Guarantee:** Within the transaction footprint, the service actively parses the `attestations` array to ensure a specific user cannot submit multiple redundant attestations for the exact same group. This guards against data duplication and unintentional upserts.
- **ArrayUnion Updates:** Image evidence URL additions (`metadata_urls`) use atomic `ArrayUnion` operations so that simultaneous uploads by multiple clients never overwrite each other.

### 2. State Syncing
- Once all unique `attestator_group` entries from the `attestations` array match the snapshotted `mandatory_attestators` prescribed in the `history` task, the transaction atomically transitions the Execution state (`attestation_status`) to `COMPLETED`. This ensures that mandatory approver groups are securely decoupled and snapshotted at the task level during initiation.
- The parent (`central_attestations`) status, `last_attested`, and structurally calculated `next_cycle_due` (monthly/quarterly/yearly base calculation) are dynamically updated within the very same transaction.

### 3. Strict Validation & Security
- Leverages `Pydantic` for stringent input/output validation handling.
- Validates the existence of the `source_type` within `attestation_definitions` prior to honoring any requests or database writes, preventing orphaned documents.

## API Endpoints Overview

1. `POST /api/v1/attestations`
   - **Purpose**: Creates the central ledger reference document initialized with a base structure and state. Must be done once per `source_type` and `reference_id` combined key prior to executing task arrays.

2. `POST /api/v1/attestations/{source_type}/{reference_id}/tasks`
   - **Purpose**: Initialize a new history task period strictly defining its `mandatory_attestators` list (like `"2026-03"`). Bound strictly to a created parent reference.
   
3. `POST /api/v1/attestations/{source_type}/{reference_id}/{period_key}/evidence`
   - **Purpose**: Upload physical evidence files, directly push to GCS, and bind the accessible Signed URL to the history document.

4. `POST /api/v1/attestations/{source_type}/{reference_id}/{period_key}/attest`
   - **Purpose**: Registers attestations transactionally in the execution history and evaluates completion state natively against the period's requirement blueprint.
