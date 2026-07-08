const ISSUER_VERIFIER_V3_TO_V4: Record<string, string> = {
  GRANTOR_VALIDATION: 'GRANTOR_VALIDATION_PROCESS',
  ECOSYSTEM: 'ECOSYSTEM_VALIDATION_PROCESS',
  OPEN: 'OPEN',
  MODE_UNSPECIFIED: 'MODE_UNSPECIFIED',
  UNKNOWN: 'UNKNOWN',
}

const ISSUER_VERIFIER_V4_TO_STORED: Record<string, string> = {
  GRANTOR_VALIDATION_PROCESS: 'GRANTOR_VALIDATION',
  ECOSYSTEM_VALIDATION_PROCESS: 'ECOSYSTEM',
  OPEN: 'OPEN',
  MODE_UNSPECIFIED: 'MODE_UNSPECIFIED',
}

export function normalizeIssuerVerifierOnboardingModeV4(mode: string | null | undefined): string {
  if (mode == null || mode === '') return 'MODE_UNSPECIFIED'
  const m = String(mode).trim()
  return ISSUER_VERIFIER_V3_TO_V4[m] ?? m
}

export function normalizeHolderOnboardingModeV4(mode: string | null | undefined): string | null {
  if (mode == null || mode === '') return null
  const m = String(mode).trim()
  if (m === 'ISSUER_VALIDATION' || m === 'ISSUER_VALIDATION_PROCESS') return 'ISSUER_VALIDATION_PROCESS'
  if (m === 'PARTICIPANTLESS' || m === 'HOLDER_PARTICIPANTLESS') return 'PARTICIPANTLESS'
  return m
}

export function normalizeIssuerVerifierOnboardingModeForDbFilter(mode: string | null | undefined): string {
  if (mode == null || mode === '') return ''
  const m = String(mode).trim()
  if (m in ISSUER_VERIFIER_V3_TO_V4) return m
  return ISSUER_VERIFIER_V4_TO_STORED[m] ?? m
}

export function mapCredentialSchemaApiFields(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row }
  const issuerSource =
    row.issuer_onboarding_mode != null && String(row.issuer_onboarding_mode).trim() !== ''
      ? String(row.issuer_onboarding_mode)
      : ''
  const verifierSource =
    row.verifier_onboarding_mode != null && String(row.verifier_onboarding_mode).trim() !== ''
      ? String(row.verifier_onboarding_mode)
      : ''
  out.issuer_onboarding_mode = normalizeIssuerVerifierOnboardingModeV4(issuerSource)
  out.verifier_onboarding_mode = normalizeIssuerVerifierOnboardingModeV4(verifierSource)
  const holderRaw = row.holder_onboarding_mode
  out.holder_onboarding_mode =
    holderRaw != null && holderRaw !== '' ? normalizeHolderOnboardingModeV4(String(holderRaw)) : null
  const { issuer_participant_management_mode, verifier_participant_management_mode, ...result } = out
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
