#!/usr/bin/env bash
#
# verana-test-flow.sh
# -------------------------------------------------------------------
# End-to-end exercise of the Verana modules against the running node.
#
# VALID FOR verana-node v0.10.1-dev.12 ONLY. If the node version changes,
# the CLI commands/flags below may differ and this file MUST be updated.
#
# Creates, on behalf of a corporation (here cooluser's own address, self-granted
# as operator):
#   - Operator authorization (de module)
#   - A Trust Registry (create, update DID, add GF doc, bump GF version, archive/unarchive)
#   - A Credential Schema (create, update validity periods, archive/unarchive)
#   - Permissions (root ECOSYSTEM, self-created ISSUER, revoke)
#
# Requires the container running (`docker compose up -d`) with cooluser funded at genesis.
# -------------------------------------------------------------------
set -euo pipefail

# ---- Config (override via env) ------------------------------------
CONTAINER="${CONTAINER:-verana-node}"
KEY="${KEY:-cooluser}"
CHAIN_ID="${CHAIN_ID:-vna-testnet-1}"
DID="${DID:-did:example:trust-registry-1}"
DID_ROTATED="${DID_ROTATED:-did:example:trust-registry-1-rotated}"
ISSUER_DID="${ISSUER_DID:-did:example:issuer-1}"
LANG_TAG="${LANG_TAG:-en}"
GF_URL="${GF_URL:-https://example.com/governance-framework.json}"

KB="--keyring-backend test --home /root/.verana"
TXFLAGS="--chain-id ${CHAIN_ID} --gas auto --gas-adjustment 1.6 --gas-prices 0.3uvna -y -o json"

# Delegable message types the operator self-grants (non-delegable msgs are omitted).
GRANT_MSGS="$(IFS=,; echo "\
/verana.tr.v1.MsgCreateTrustRegistry \
/verana.tr.v1.MsgUpdateTrustRegistry \
/verana.tr.v1.MsgAddGovernanceFrameworkDocument \
/verana.tr.v1.MsgIncreaseActiveGovernanceFrameworkVersion \
/verana.tr.v1.MsgArchiveTrustRegistry \
/verana.cs.v1.MsgCreateCredentialSchema \
/verana.cs.v1.MsgUpdateCredentialSchema \
/verana.cs.v1.MsgArchiveCredentialSchema \
/verana.perm.v1.MsgCreateRootPermission \
/verana.perm.v1.MsgSelfCreatePermission \
/verana.perm.v1.MsgRevokePermission" | tr -s ' ' ',')"

# ---- Helpers ------------------------------------------------------
v() { docker exec -i "$CONTAINER" veranad "$@"; }
c() { docker exec -i "$CONTAINER" "$@"; }
section() { echo; echo "==== $* ===="; }

# Compute a valid sha384 Subresource Integrity digest in-container.
sri() { echo "sha384-$(c python3 -c "import hashlib,base64,sys;print(base64.b64encode(hashlib.sha384(sys.argv[1].encode()).digest()).decode())" "$1" | tr -d '\r')"; }

# Broadcast a tx, wait until committed, fail on code!=0.
submit() {
  local desc="$1"; shift
  echo ">> ${desc}"
  local out txhash code res
  out="$(v "$@" ${KB} ${TXFLAGS} 2>&1)" || { echo "$out"; exit 1; }
  txhash="$(echo "$out" | grep -o '"txhash":"[^"]*"' | head -1 | cut -d'"' -f4)"
  if [ -z "$txhash" ]; then echo "   ERROR: no txhash returned:"; echo "$out"; exit 1; fi
  for _ in $(seq 1 20); do
    sleep 2
    res="$(v query tx "$txhash" -o json 2>/dev/null || true)"
    code="$(echo "$res" | jq -r '.code // empty' 2>/dev/null || true)"
    [ -z "$code" ] && continue
    if [ "$code" = "0" ]; then echo "   OK  (tx ${txhash})"; return 0; fi
    echo "   FAILED (code ${code}): $(echo "$res" | jq -r '.raw_log')"; exit 1
  done
  echo "   ERROR: timed out waiting for tx ${txhash}"; exit 1
}

# Latest-id helpers (entities are id-monotonic).
last_tr()     { v query tr list-trust-registries -o json | jq -r '.trust_registries | max_by(.id|tonumber) | .id'; }
last_schema() { v query cs list-schemas          -o json | jq -r '.schemas | max_by(.id|tonumber) | .id'; }
last_perm()   { v query perm list-permissions    -o json | jq -r --arg t "$1" '[.permissions[]|select(.type==$t)] | max_by(.id|tonumber) | .id'; }

# ---- Preflight ----------------------------------------------------
if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
  echo "ERROR: container '$CONTAINER' is not running. Start it with: docker compose up -d"
  exit 1
fi

ADDR="$(v keys show "$KEY" -a ${KB})"
echo "Signer (operator & corporation): ${KEY} = ${ADDR}"

# ============================================================
section "1. Operator authorization"
# Self-grant: corporation = signer, operator omitted, grantee = self.
submit "Granting operator authorization to ${KEY}" \
  tx de grant-operator-authz "$ADDR" --msg-types "$GRANT_MSGS" --from "$KEY"
v query de list-operator-authorizations -o json | jq -c '.operator_authorizations[] | {corporation, operator, msg_types: (.msg_types|length)}'

# ============================================================
section "2. Trust Registry lifecycle"
submit "Create Trust Registry for ${DID}" \
  tx tr create-trust-registry "$ADDR" "$DID" "$LANG_TAG" "$GF_URL" "$(sri 'gf v1')" --from "$KEY"
TR_ID="$(last_tr)"; echo "   Trust Registry ID = ${TR_ID}"

submit "Rotate the Trust Registry DID -> ${DID_ROTATED}" \
  tx tr update-trust-registry "$ADDR" "$TR_ID" "$DID_ROTATED" --from "$KEY"

ACTIVE_GF="$(v query tr get-trust-registry "$TR_ID" -o json | jq -r '(.trust_registry // .).active_version')"
NEXT_GF=$((ACTIVE_GF + 1))
submit "Add governance framework document v${NEXT_GF}" \
  tx tr add-governance-framework-document "$ADDR" "$TR_ID" "$LANG_TAG" \
     "https://example.com/gf-v${NEXT_GF}.json" "$(sri "gf v${NEXT_GF}")" "$NEXT_GF" --from "$KEY"

submit "Activate governance framework version ${NEXT_GF}" \
  tx tr increase-active-gf-version "$ADDR" "$TR_ID" --from "$KEY"

submit "Archive the Trust Registry"   tx tr archive-trust-registry "$ADDR" "$TR_ID" true  --from "$KEY"
submit "Unarchive the Trust Registry" tx tr archive-trust-registry "$ADDR" "$TR_ID" false --from "$KEY"

# ============================================================
section "3. Credential Schema lifecycle"
# Modes 1=OPEN/2=GRANTOR_VALIDATION/3=ECOSYSTEM; asset-type 1=TU "tu"; digest sha256; validity periods are OptionalUInt32 {"value":N}.
SCHEMA_JSON='{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","title":"ExampleCredential","description":"Example credential schema for testing","properties":{"name":{"type":"string"},"country":{"type":"string"}},"required":["name"]}'
VP='{"value":365}'
submit "Create Credential Schema under Trust Registry ${TR_ID} (issuer/verifier mode OPEN)" \
  tx cs create-credential-schema "$TR_ID" "$SCHEMA_JSON" 1 1 1 1 tu sha256 \
     --corporation "$ADDR" \
     --issuer-grantor-validation-validity-period "$VP" \
     --verifier-grantor-validation-validity-period "$VP" \
     --issuer-validation-validity-period "$VP" \
     --verifier-validation-validity-period "$VP" \
     --holder-validation-validity-period "$VP" --from "$KEY"
SCHEMA_ID="$(last_schema)"; echo "   Credential Schema ID = ${SCHEMA_ID}"

# Update requires all five validity periods (same as create).
submit "Update Credential Schema validity periods" \
  tx cs update "$SCHEMA_ID" --corporation "$ADDR" \
     --issuer-grantor-validation-validity-period "$VP" \
     --verifier-grantor-validation-validity-period "$VP" \
     --issuer-validation-validity-period '{"value":180}' \
     --verifier-validation-validity-period "$VP" \
     --holder-validation-validity-period "$VP" --from "$KEY"

submit "Archive the Credential Schema"   tx cs archive "$SCHEMA_ID" true  --corporation "$ADDR" --from "$KEY"
submit "Unarchive the Credential Schema" tx cs archive "$SCHEMA_ID" false --corporation "$ADDR" --from "$KEY"

# ============================================================
section "4. Permissions"
# Root (ECOSYSTEM) permission for the schema; controller-only.
EFFECTIVE_FROM="$(c date -u -d '+2 minutes' +%Y-%m-%dT%H:%M:%SZ | tr -d '\r')"
submit "Create root permission for schema ${SCHEMA_ID} (effective ${EFFECTIVE_FROM})" \
  tx perm create-root-perm "$SCHEMA_ID" "$DID" 0 0 0 \
     --corporation "$ADDR" --effective-from "$EFFECTIVE_FROM" --from "$KEY"
ROOT_PERM_ID="$(last_perm ECOSYSTEM)"; echo "   Root (ECOSYSTEM) permission ID = ${ROOT_PERM_ID}"

# Self-created ISSUER permission (allowed because the schema is OPEN); [type] is the enum name in lowercase.
submit "Self-create an ISSUER permission under root ${ROOT_PERM_ID}" \
  tx perm self-create-perm issuer "$ROOT_PERM_ID" "$ISSUER_DID" \
     --corporation "$ADDR" --from "$KEY"
ISSUER_PERM_ID="$(last_perm ISSUER)"; echo "   ISSUER permission ID = ${ISSUER_PERM_ID}"
v query perm get-perm "$ISSUER_PERM_ID" -o json | jq '(.permission // .) | {id, schema_id, type, did, validator_perm_id, grantee}'

submit "Revoke ISSUER permission ${ISSUER_PERM_ID}" \
  tx perm revoke-perm "$ISSUER_PERM_ID" --corporation "$ADDR" --from "$KEY"

# ============================================================
section "RESULT"
echo "Trust Registry ${TR_ID}:"
v query tr get-trust-registry "$TR_ID" -o json | jq '(.trust_registry // .) | {id, did, active_version, archived}'
echo "Credential Schema ${SCHEMA_ID}:"
v query cs get-schema "$SCHEMA_ID" -o json | jq '(.schema // .) | {id, tr_id, archived}'
echo "Permissions for schema ${SCHEMA_ID}:"
v query perm list-permissions -o json | jq --argjson s "$SCHEMA_ID" '.permissions[] | select((.schema_id|tonumber)==$s) | {id, type, did, revoked}'
echo
echo "All steps completed."
