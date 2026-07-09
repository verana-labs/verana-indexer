export function mapCredentialSchemaApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const { issuer_participant_management_mode, verifier_participant_management_mode, ...result } = row
  return result
}

export function mapEcosystemApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row }
  out.corporation_id = Number(out.corporation_id ?? 0) || 0
  const { corporation, controller, aka, ...result } = out
  return result
}

export function normalizeVprFeeDiscountRatio(v: unknown): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return 0
  if (n > 1) return Math.min(1, n / 10000)
  return Math.min(1, Math.max(0, n))
}

const PARTICIPANT_HIDDEN_COLUMNS = [
  'corporation',
  'vs_operator_authz_enabled',
  'vs_operator_authz_spend_limit',
  'vs_operator_authz_with_feegrant',
  'vs_operator_authz_fee_spend_limit',
  'vs_operator_authz_spend_period',
] as const

export function mapParticipantApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row }
  out.corporation_id = Number(out.corporation_id ?? 0) || 0
  for (const k of PARTICIPANT_HIDDEN_COLUMNS) {
    delete out[k]
  }
  if ('issuance_fee_discount' in out) {
    out.issuance_fee_discount = normalizeVprFeeDiscountRatio(out.issuance_fee_discount)
  }
  if ('verification_fee_discount' in out) {
    out.verification_fee_discount = normalizeVprFeeDiscountRatio(out.verification_fee_discount)
  }
  return out
}

export function mapTrustDepositApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const { account, amount, last_repaid_by, ...out } = row
  return out
}
