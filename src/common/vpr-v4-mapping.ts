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

const VPR_FEE_DISCOUNT_CHAIN_SCALE = 10000

export function normalizeVprFeeDiscountRatio(v: unknown): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return 0
  return Math.min(1, Math.max(0, n / VPR_FEE_DISCOUNT_CHAIN_SCALE))
}

const PARTICIPANT_HIDDEN_COLUMNS = [
  'corporation',
  'vs_operator_authz_enabled',
  'vs_operator_authz_spend_limit',
  'vs_operator_authz_with_feegrant',
  'vs_operator_authz_fee_spend_limit',
  'vs_operator_authz_spend_period',
] as const

const PARTICIPANT_FEE_DISCOUNT_FIELDS = ['issuance_fee_discount', 'verification_fee_discount'] as const

export function normalizeVprFeeDiscountFields<T extends Record<string, unknown>>(target: T): T {
  for (const field of PARTICIPANT_FEE_DISCOUNT_FIELDS) {
    if (field in target) {
      ;(target as Record<string, unknown>)[field] = normalizeVprFeeDiscountRatio(target[field])
    }
  }
  return target
}

export function mapParticipantApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row }
  out.corporation_id = Number(out.corporation_id ?? 0) || 0
  for (const k of PARTICIPANT_HIDDEN_COLUMNS) {
    delete out[k]
  }
  return normalizeVprFeeDiscountFields(out)
}

export function mapTrustDepositApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const { account, amount, last_repaid_by, ...out } = row
  return out
}
