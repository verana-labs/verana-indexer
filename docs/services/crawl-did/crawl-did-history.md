# DID History Service

The **DID History Service** is responsible for recording all lifecycle changes
(add, renew, touch, remove) of Decentralized Identifiers (DIDs) and allowing
clients to query historical data for a given DID.

---

## 📦 How it works
1. Incoming DID-related blockchain messages are processed by
   `DidMessageProcessorService`.
2. For each message:
   - The current DID state is **upserted** in the `DidDatabaseService`.
   - A **history record** is created and persisted through
     `DidHistoryService.save()`.
3. This creates an append-only audit trail of all DID changes over time.

---

## 🗂️ Data Saved

Each DID history record includes:

- `did` — DID identifier string.
- `controller` — Current controller of the DID.
- `deposit` — Deposit amount associated with the DID.
- `exp` — Expiration date of the DID.
- `created` — Initial creation timestamp.
- `changes` — JSON object describing differences compared to previous record.   
---

## 🔎 Querying DID History

### Get all history for a DID

**Endpoint**

```http
GET http://localhost:3001/verana/dd/v1/history/did:example:184a2fddab1b3d505d477adbf0643446


```json
{
  "success": true,
  "data": [
    {
      "did": "did:example:184a2fddab1b3d505d477adbf0643446",
      "years": "1",
      "controller": "verana1k6exwj6644xy028vxtzxs2fhf9nt8hymeuqkz7",
      "deposit": "5000000",
      "exp": "2026-06-27T05:16:00.010+05:00",
      "created": "2025-06-27T00:16:00.010Z",
      "changes": {
        "modified": {
          "old": "2025-06-28T02:31:15.539Z",
          "new": "2025-06-29T04:42:10.123Z"
        }
      }
    },
    {
      "did": "did:example:184a2fddab1b3d505d477adbf0643446",
      "years": "1",
      "controller": "verana12dyk649yce4dvdppehsyraxe6p6jemzg2qwutf",
      "deposit": "5000000",
      "exp": "2026-06-18T16:27:29.384Z",
      "created": "2025-06-18T16:27:29.384Z",
      "changes": {}
    }
  ]
}
