import { toDate } from '../../common/utils/date_utils'

export type ParticipantState = 'REPAID' | 'SLASHED' | 'REVOKED' | 'EXPIRED' | 'ACTIVE' | 'FUTURE' | 'INACTIVE'

export type CorporationAction =
  | 'OP_RENEW'
  | 'OP_CANCEL'
  | 'PARTICIPANT_REVOKE'
  | 'PARTICIPANT_ADJUST'
  | 'PARTICIPANT_REPAY'
export type GranteeAction = CorporationAction
export type ValidatorAction = 'OP_SET_VALIDATED' | 'PARTICIPANT_REVOKE' | 'PARTICIPANT_ADJUST' | 'PARTICIPANT_SLASH'

const PARTICIPANT_ACTION_TO_VPR_MESSAGE: Record<CorporationAction | ValidatorAction, string> = {
  OP_RENEW: 'RenewParticipantOP',
  OP_CANCEL: 'CancelParticipantOPLastRequest',
  OP_SET_VALIDATED: 'SetParticipantOPtoValidated',
  PARTICIPANT_REVOKE: 'RevokeParticipant',
  PARTICIPANT_ADJUST: 'SetParticipantEffectiveUntil',
  PARTICIPANT_REPAY: 'RepayParticipantSlashedTrustDeposit',
  PARTICIPANT_SLASH: 'SlashParticipantTrustDeposit',
}

export function mapParticipantActionsToVprMessages(actions: (CorporationAction | ValidatorAction)[]): string[] {
  return actions.map((action) => PARTICIPANT_ACTION_TO_VPR_MESSAGE[action] ?? action)
}

export type ParticipantType =
  | 'UNSPECIFIED'
  | 'ISSUER_GRANTOR'
  | 'ISSUER'
  | 'VERIFIER_GRANTOR'
  | 'VERIFIER'
  | 'HOLDER'
  | 'ECOSYSTEM'
export type ValidationState = 'VALIDATION_STATE_UNSPECIFIED' | 'PENDING' | 'VALIDATED' | 'TERMINATED' | null
export type SchemaMode = 'GRANTOR_ONBOARDING_PROCESS' | 'OPEN' | 'ECOSYSTEM_ONBOARDING_PROCESS'

export interface ParticipantData {
  repaid?: string | null
  slashed?: string | null
  revoked?: string | null
  effective_from?: string | null
  effective_until?: string | null
  role: ParticipantType
  op_state?: ValidationState
  op_exp?: string | null
  validator_participant_id?: string | null
}

export interface SchemaData {
  issuer_onboarding_mode?: string
  verifier_onboarding_mode?: string
}

export const PENDING_FLAT_OP_PENDING_PARTICIPANT_STATES: ReadonlySet<ParticipantState> = new Set([
  'INACTIVE',
  'ACTIVE',
  'FUTURE',
  'EXPIRED',
])

export function pendingFlatMatchesOpPendingWithEligibleParticipantState(participant: {
  op_state?: string | null
  participant_state?: string | null
}): boolean {
  if (String(participant.op_state ?? '').toUpperCase() !== 'PENDING') return false
  const ps = participant.participant_state as ParticipantState | undefined
  return ps !== undefined && PENDING_FLAT_OP_PENDING_PARTICIPANT_STATES.has(ps)
}

export const PENDING_FLAT_VALIDATOR_PARENT_TYPES: ReadonlySet<string> = new Set([
  'ISSUER_GRANTOR',
  'VERIFIER_GRANTOR',
  'ECOSYSTEM',
  'ISSUER',
])

export function calculateParticipantState(participant: ParticipantData, now: Date = new Date()): ParticipantState {
  const slashed = toDate(participant.slashed)
  const repaid = toDate(participant.repaid)
  const revoked = toDate(participant.revoked)
  const effectiveFrom = toDate(participant.effective_from)
  const effectiveUntil = toDate(participant.effective_until)

  if (slashed) return repaid && repaid >= slashed ? 'REPAID' : 'SLASHED'
  if (revoked && revoked <= now) return 'REVOKED'
  if (effectiveUntil && effectiveUntil <= now) return 'EXPIRED'
  if (effectiveFrom) return effectiveFrom > now ? 'FUTURE' : 'ACTIVE'
  return isOnboardingPending(participant.op_state) ? 'INACTIVE' : 'ACTIVE'
}

export const ONBOARDING_PENDING_OP_STATES: readonly string[] = ['PENDING', 'TERMINATED']

export function isOnboardingPending(opState?: ValidationState | string | number | null): boolean {
  const normalized = normalizeOpState(opState)
  return normalized !== null && ONBOARDING_PENDING_OP_STATES.includes(normalized)
}

export function applyActiveEffectiveFromFilter(
  query: any,
  nowIso: string,
  col: (name: string) => string = (name) => name
): void {
  query.where((qb: any) =>
    qb
      .where((started: any) =>
        started.whereNotNull(col('effective_from')).andWhere(col('effective_from'), '<=', nowIso)
      )
      .orWhere((neverScheduled: any) =>
        neverScheduled
          .whereNull(col('effective_from'))
          .andWhere((onboarded: any) =>
            onboarded.whereNull(col('op_state')).orWhereNotIn(col('op_state'), ONBOARDING_PENDING_OP_STATES)
          )
      )
  )
}

export function applyActiveParticipantFilter(
  query: any,
  nowIso: string,
  col: (name: string) => string = (name) => name
): void {
  query.where((qb: any) => {
    qb.whereNull(col('slashed'))
    qb.where((notRevoked: any) => notRevoked.whereNull(col('revoked')).orWhere(col('revoked'), '>', nowIso))
    qb.where((notEnded: any) => notEnded.whereNull(col('effective_until')).orWhere(col('effective_until'), '>', nowIso))
    applyActiveEffectiveFromFilter(qb, nowIso, col)
  })
}

export function applyParticipantStateFilter(
  query: any,
  state: ParticipantState,
  nowIso: string,
  col: (name: string) => string = (name) => name
): void {
  if (state === 'ACTIVE') {
    applyActiveParticipantFilter(query, nowIso, col)
    return
  }
  const notRevoked = (qb: any) => qb.whereNull(col('revoked')).orWhere(col('revoked'), '>', nowIso)
  const notEnded = (qb: any) => qb.whereNull(col('effective_until')).orWhere(col('effective_until'), '>', nowIso)
  query.where((qb: any) => {
    switch (state) {
      case 'REPAID':
        qb.whereNotNull(col('slashed')).whereRaw('?? >= ??', [col('repaid'), col('slashed')])
        return
      case 'SLASHED':
        qb.whereNotNull(col('slashed')).where((repay: any) =>
          repay.whereNull(col('repaid')).orWhereRaw('?? < ??', [col('repaid'), col('slashed')])
        )
        return
      case 'REVOKED':
        qb.whereNull(col('slashed')).where(col('revoked'), '<=', nowIso)
        return
      case 'EXPIRED':
        qb.whereNull(col('slashed')).where(notRevoked).where(col('effective_until'), '<=', nowIso)
        return
      case 'FUTURE':
        qb.whereNull(col('slashed')).where(notRevoked).where(notEnded).where(col('effective_from'), '>', nowIso)
        return
      case 'INACTIVE':
        qb.whereNull(col('slashed'))
          .where(notRevoked)
          .where(notEnded)
          .whereNull(col('effective_from'))
          .whereIn(col('op_state'), ONBOARDING_PENDING_OP_STATES)
        return
    }
  })
}

function normalizeSchemaMode(mode?: string): SchemaMode {
  if (!mode) return 'OPEN'
  const upper = mode.toUpperCase()
  if (upper === 'GRANTOR_ONBOARDING_PROCESS') return 'GRANTOR_ONBOARDING_PROCESS'
  if (upper === 'ECOSYSTEM_ONBOARDING_PROCESS') return 'ECOSYSTEM_ONBOARDING_PROCESS'
  return 'OPEN'
}

export function normalizeParticipantType(value: unknown): ParticipantType {
  if (typeof value === 'string') {
    const upper = value.toUpperCase()
    if (
      upper === 'UNSPECIFIED' ||
      upper === 'ISSUER' ||
      upper === 'VERIFIER' ||
      upper === 'ISSUER_GRANTOR' ||
      upper === 'VERIFIER_GRANTOR' ||
      upper === 'ECOSYSTEM' ||
      upper === 'HOLDER'
    ) {
      return upper as ParticipantType
    }
  }
  const n = Number(value)
  switch (n) {
    case 0:
      return 'UNSPECIFIED'
    case 1:
      return 'ISSUER'
    case 2:
      return 'VERIFIER'
    case 3:
      return 'ISSUER_GRANTOR'
    case 4:
      return 'VERIFIER_GRANTOR'
    case 5:
      return 'ECOSYSTEM'
    case 6:
      return 'HOLDER'
    default:
      return 'UNSPECIFIED'
  }
}

function normalizeOpState(value: unknown): ValidationState {
  if (value === null || value === undefined) return null
  if (typeof value === 'string') {
    const upper = value.toUpperCase()
    if (upper === 'VALIDATION_STATE_UNSPECIFIED' || upper === 'UNSPECIFIED') return 'VALIDATION_STATE_UNSPECIFIED'
    if (upper === 'PENDING') return 'PENDING'
    if (upper === 'VALIDATED') return 'VALIDATED'
    if (upper === 'TERMINATED' || upper === 'TERMINATION_REQUESTED') return 'TERMINATED'
    return null
  }
  const n = Number(value)
  if (n === 1) return 'PENDING'
  if (n === 2) return 'VALIDATED'
  if (n === 3 || n === 4) return 'TERMINATED'
  return 'VALIDATION_STATE_UNSPECIFIED'
}

function isIssuerType(type: ParticipantType): boolean {
  return type === 'ISSUER_GRANTOR' || type === 'ISSUER'
}

function isVerifierType(type: ParticipantType): boolean {
  return type === 'VERIFIER_GRANTOR' || type === 'VERIFIER'
}

function addOnboardingProcessCorporationActions(
  actions: Set<CorporationAction>,
  participantState: ParticipantState,
  opState: ValidationState,
  isValidatorActive: boolean
): void {
  if (participantState !== 'ACTIVE' && participantState !== 'FUTURE' && participantState !== 'INACTIVE') return
  const canRevoke = participantState === 'ACTIVE' || participantState === 'FUTURE'
  if (opState === 'VALIDATED') {
    if (isValidatorActive) {
      actions.add('OP_RENEW')
    }
    if (canRevoke) {
      actions.add('PARTICIPANT_REVOKE')
    }
  } else if (opState === 'PENDING') {
    actions.add('OP_CANCEL')
    if (canRevoke) {
      actions.add('PARTICIPANT_REVOKE')
    }
  }
}

function addSelfManagedCorporationActions(actions: Set<CorporationAction>, participantState: ParticipantState): void {
  if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
    actions.add('PARTICIPANT_REVOKE')
    actions.add('PARTICIPANT_ADJUST')
  }
}

export function calculateCorporationAvailableActions(
  participant: ParticipantData,
  schema: SchemaData,
  validatorParticipantState?: ParticipantState | null,
  now: Date = new Date()
): CorporationAction[] {
  const actions: Set<CorporationAction> = new Set()
  const type = normalizeParticipantType(participant.role)
  const opState = normalizeOpState(participant.op_state)
  const participantState = calculateParticipantState(participant, now)
  const issuerMode = normalizeSchemaMode(schema.issuer_onboarding_mode)
  const verifierMode = normalizeSchemaMode(schema.verifier_onboarding_mode)
  const isValidatorActive = validatorParticipantState === 'ACTIVE'

  if (participantState === 'SLASHED' && type !== 'UNSPECIFIED') {
    actions.add('PARTICIPANT_REPAY')
  }

  if (isIssuerType(type)) {
    if (issuerMode === 'OPEN') {
      if (type === 'ISSUER') {
        addSelfManagedCorporationActions(actions, participantState)
      }
    } else {
      addOnboardingProcessCorporationActions(actions, participantState, opState, isValidatorActive)
    }
  }

  if (isVerifierType(type)) {
    if (verifierMode === 'OPEN') {
      if (type === 'VERIFIER') {
        addSelfManagedCorporationActions(actions, participantState)
      }
    } else {
      addOnboardingProcessCorporationActions(actions, participantState, opState, isValidatorActive)
    }
  }

  if (type === 'HOLDER') {
    addOnboardingProcessCorporationActions(actions, participantState, opState, isValidatorActive)
  }

  if (type === 'ECOSYSTEM') {
    addSelfManagedCorporationActions(actions, participantState)
  }

  return Array.from(actions).sort()
}

export const calculateGranteeAvailableActions = calculateCorporationAvailableActions

export function calculateValidatorAvailableActions(
  participant: ParticipantData,
  schema: SchemaData,
  now: Date = new Date()
): ValidatorAction[] {
  const type = normalizeParticipantType(participant.role)
  if (!isIssuerType(type) && !isVerifierType(type) && type !== 'HOLDER') return []
  if (isIssuerType(type) && normalizeSchemaMode(schema.issuer_onboarding_mode) === 'OPEN') return []
  if (isVerifierType(type) && normalizeSchemaMode(schema.verifier_onboarding_mode) === 'OPEN') return []

  const actions: Set<ValidatorAction> = new Set(['PARTICIPANT_SLASH'])
  const opState = normalizeOpState(participant.op_state)
  const participantState = calculateParticipantState(participant, now)

  if (participantState === 'ACTIVE') {
    actions.add('PARTICIPANT_REVOKE')
    actions.add('PARTICIPANT_ADJUST')
  }

  if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
    if (opState === 'PENDING') {
      actions.add('OP_SET_VALIDATED')
    }
  }

  return Array.from(actions).sort()
}
