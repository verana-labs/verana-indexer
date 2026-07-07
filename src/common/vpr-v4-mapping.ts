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

const PARTICIPANT_V3_STRIP = [
  'grantee',
  'authority',
  'created_by',
  'revoked_by',
  'slashed_by',
  'repaid_by',
  'extended',
  'extended_by',
  'op_term_requested',
  'op_summary_digest_sri',
  'country',
] as const

export function mapParticipantApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row }
  out.corporation_id = Number(out.corporation_id ?? 0) || 0
  for (const k of [...PARTICIPANT_V3_STRIP, 'corporation']) {
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
