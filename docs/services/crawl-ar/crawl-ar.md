# 🧾 Account Reputation Query API

This document describes the API for **Account Reputation** ([MIXED-QRY-1]) according to the system specification.

---

## 📌 [MIXED-QRY-1] Get Account Reputation

Get the reputation of an account (trust deposit, slashes, repayments, trust registry and credential schema stats).

Any account can run this query.

---

### 📥 **Endpoint**

```
GET /mx/v1/reputation
```

### 🧭 **Method**

`GET` — Exposed via API Gateway as a route alias that calls the Moleculer action

> **Action name:** `v1.AccountReputationService.getAccountReputation`

---

### 🧾 **Query Parameters**

| Name                    | Type      | Required | Description                                                                                            |
| ----------------------- | --------- | -------- | ------------------------------------------------------------------------------------------------------ |
| `account`               | `string`  | ✅       | Account address (e.g. `verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st`)                                 |
| `tr_id`                 | `number`  | ❌       | Filter by Trust Registry ID                                                                            |
| `schema_id`             | `number`  | ❌       | Filter by Credential Schema ID                                                                         |
| `include_slash_details` | `boolean` | ❌       | If true, includes detailed slash and repayment records (top-level and per schema). Defaults to `false` |

---

### 🧠 **Behavior**

- If `account` is not found → returns 404.
- If `tr_id` or `schema_id` are specified → filters returned data.
- If `include_slash_details` is `true` → includes arrays of slash and repayment records.

---

### 📤 **Response Example**

#### ✅ Success (HTTP 200)

```json
{
  "account": "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
  "balance": "1000000",
  "deposit": "500000",
  "slashed": "100000",
  "repaid": "50000",
  "slash_count": 2,
  "first_interaction_ts": "2025-01-01T10:20:30.000Z",
  "trust_registry_count": 1,
  "credential_schema_count": 2,
  "slashs": [
    {
      "slashed_amount": "50000",
      "slashed_ts": "2025-02-01T12:00:00Z",
      "slashed_by": "verana1xxxx..."
    }
  ],
  "repayments": [
    {
      "repaid_amount": "50000",
      "repaid_ts": "2025-03-01T12:00:00Z",
      "repaid_by": "verana1xxxx..."
    }
  ],
  "trust_registries": [
    {
      "tr_id": 1,
      "tr_did": "did:verana:registry:123",
      "credential_schemas": [
        {
          "schema_id": 10,
          "deposit": "300000",
          "slashed": "0",
          "repaid": "0",
          "slash_count": 0,
          "issued": 12,
          "verified": 6,
          "run_as_validator_vps": 3,
          "run_as_applicant_vps": 5,
          "issuer_perm_count": 2,
          "verifier_perm_count": 3,
          "issuer_grantor_perm_count": 0,
          "verifier_grantor_perm_count": 1,
          "ecosystem_perm_count": 1,
          "active_issuer_perm_count": 2,
          "active_verifier_perm_count": 3,
          "active_issuer_grantor_perm_count": 0,
          "active_verifier_grantor_perm_count": 1,
          "active_ecosystem_perm_count": 1,
          "slashs": [],
          "repayments": []
        },
        {
          "schema_id": 11,
          "deposit": "200000",
          "slashed": "100000",
          "repaid": "50000",
          "slash_count": 2,
          "issued": 4,
          "verified": 2,
          "run_as_validator_vps": 1,
          "run_as_applicant_vps": 1,
          "issuer_perm_count": 1,
          "verifier_perm_count": 1,
          "issuer_grantor_perm_count": 0,
          "verifier_grantor_perm_count": 0,
          "ecosystem_perm_count": 1,
          "active_issuer_perm_count": 1,
          "active_verifier_perm_count": 1,
          "active_issuer_grantor_perm_count": 0,
          "active_verifier_grantor_perm_count": 0,
          "active_ecosystem_perm_count": 1,
          "slashs": [
            {
              "perm_id": "uuid-abc-123",
              "schema_id": 11,
              "tr_id": 1,
              "slashed_ts": "2025-02-01T12:00:00Z",
              "slashed_by": "verana1yyyy..."
            }
          ],
          "repayments": [
            {
              "perm_id": "uuid-def-456",
              "schema_id": 11,
              "tr_id": 1,
              "repaid_ts": "2025-03-01T12:00:00Z",
              "repaid_by": "verana1zzzz..."
            }
          ]
        }
      ]
    }
  ]
}
```

---

#### ❌ Error (HTTP 404)

```json
{
  "error": "Account verana1xxxx not found"
}
```

---

### 🧪 Example `curl` Commands

> Note: this action is exposed as a GET route under the API gateway. Provide parameters as query string.

#### ✅ 1. Get full account reputation

```bash
curl -G "http://localhost:3001/mx/v1/reputation" \
  --data-urlencode "account=verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st" \
  --data-urlencode "include_slash_details=true"
```

#### ✅ 2. Filter by Trust Registry ID

```bash
curl -G "http://localhost:3001/mx/v1/reputation" \
  --data-urlencode "account=verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st" \
  --data-urlencode "tr_id=1"
```

#### ✅ 3. Filter by Schema ID only

```bash
curl -G "http://localhost:3001/mx/v1/reputation" \
  --data-urlencode "account=verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st" \
  --data-urlencode "schema_id=11"
```

#### ✅ 4. Filter by Trust Registry ID + Schema ID with details

```bash
curl -G "http://localhost:3001/mx/v1/reputation" \
  --data-urlencode "account=verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st" \
  --data-urlencode "tr_id=1" \
  --data-urlencode "schema_id=11" \
  --data-urlencode "include_slash_details=true"
```
