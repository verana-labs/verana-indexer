export type ParticipantState = 'REPAID' | 'SLASHED' | 'REVOKED' | 'EXPIRED' | 'ACTIVE' | 'FUTURE' | 'INACTIVE'

export type CorporationAction =
  | 'OP_RENEW'
  | 'OP_CANCEL'
  | 'PARTICIPANT_REVOKE'
  | 'PARTICIPANT_ADJUST'
  | 'PARTICIPANT_REPAY'
export type GranteeAction = CorporationAction
export type ValidatorAction = 'OP_SET_VALIDATED' | 'PARTICIPANT_REVOKE' | 'PARTICIPANT_ADJUST' | 'PARTICIPANT_SLASH'

export type ParticipantType =
  | 'UNSPECIFIED'
  | 'ISSUER_GRANTOR'
  | 'ISSUER'
  | 'VERIFIER_GRANTOR'
  | 'VERIFIER'
  | 'HOLDER'
  | 'ECOSYSTEM'
export type ValidationState = 'VALIDATION_STATE_UNSPECIFIED' | 'PENDING' | 'VALIDATED' | 'TERMINATED' | null
export type SchemaMode = 'GRANTOR_VALIDATION' | 'OPEN' | 'ECOSYSTEM'

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
  if (participant.repaid !== null && participant.repaid !== undefined) {
    return 'REPAID'
  }

  if (participant.slashed !== null && participant.slashed !== undefined) {
    return 'SLASHED'
  }

  if (participant.revoked !== null && participant.revoked !== undefined) {
    const revokedDate = new Date(participant.revoked)
    if (!Number.isNaN(revokedDate.getTime()) && revokedDate < now) {
      return 'REVOKED'
    }
  }

  if (participant.effective_until !== null && participant.effective_until !== undefined) {
    const untilDate = new Date(participant.effective_until)
    if (!Number.isNaN(untilDate.getTime()) && untilDate < now) {
      return 'EXPIRED'
    }
  }

  if (participant.effective_from !== null && participant.effective_from !== undefined) {
    const fromDate = new Date(participant.effective_from)
    if (!Number.isNaN(fromDate.getTime())) {
      if (fromDate <= now) {
        return 'ACTIVE'
      }
      return 'FUTURE'
    }
  }

  return 'INACTIVE'
}

function normalizeSchemaMode(mode?: string): SchemaMode {
  if (!mode) return 'OPEN'
  const upper = mode.toUpperCase()
  if (upper === 'GRANTOR_VALIDATION' || upper === 'GRANTOR' || upper === 'GRANTOR_VALIDATION_PROCESS') {
    return 'GRANTOR_VALIDATION'
  }
  if (upper === 'OPEN') return 'OPEN'
  if (upper === 'ECOSYSTEM' || upper === 'ECOSYSTEM_VALIDATION_PROCESS') return 'ECOSYSTEM'
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
  const opExp = participant.op_exp ? new Date(participant.op_exp) : null
  const isOpExpired = opExp !== null && !Number.isNaN(opExp.getTime()) && opExp < now

  const isValidatorActive = validatorParticipantState === 'ACTIVE'
  if (isIssuerType(type)) {
    if (issuerMode === 'GRANTOR_VALIDATION' || issuerMode === 'ECOSYSTEM') {
      if (participantState === 'REPAID' || participantState === 'REVOKED') {
      } else if (participantState === 'SLASHED') {
        actions.add('PARTICIPANT_REPAY')
      } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        if (opState === 'VALIDATED' && !isOpExpired) {
          if (isValidatorActive) {
            actions.add('OP_RENEW')
          }
          if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
            actions.add('PARTICIPANT_REVOKE')
          }
        } else if (opState === 'PENDING') {
          actions.add('OP_CANCEL')
          if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
            actions.add('PARTICIPANT_REVOKE')
          }
        }
      }
    } else if (issuerMode === 'OPEN') {
      if (participantState === 'REPAID' || participantState === 'REVOKED') {
      } else if (participantState === 'SLASHED') {
        actions.add('PARTICIPANT_REPAY')
      } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        actions.add('PARTICIPANT_REVOKE')
        actions.add('PARTICIPANT_ADJUST')
      }
    }
  }

  if (isVerifierType(type)) {
    const inOpFlow = opState !== null && opState !== 'VALIDATION_STATE_UNSPECIFIED'
    const useOpFlowRules = verifierMode === 'GRANTOR_VALIDATION' || verifierMode === 'ECOSYSTEM' || inOpFlow
    if (useOpFlowRules) {
      if (participantState === 'REPAID' || participantState === 'REVOKED') {
      } else if (participantState === 'SLASHED') {
        actions.add('PARTICIPANT_REPAY')
      } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        if (opState === 'VALIDATED' && !isOpExpired) {
          if (isValidatorActive) {
            actions.add('OP_RENEW')
          }
          if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
            actions.add('PARTICIPANT_REVOKE')
          }
        } else if (opState === 'PENDING') {
          actions.add('OP_CANCEL')
          if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
            actions.add('PARTICIPANT_REVOKE')
          }
        }
      }
    } else if (verifierMode === 'OPEN' && !inOpFlow) {
      if (participantState === 'REPAID' || participantState === 'REVOKED') {
      } else if (participantState === 'SLASHED') {
        actions.add('PARTICIPANT_REPAY')
      } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        actions.add('PARTICIPANT_REVOKE')
        actions.add('PARTICIPANT_ADJUST')
      }
    }
  }

  if (type === 'HOLDER') {
    if (participantState === 'REPAID' || participantState === 'REVOKED') {
    } else if (participantState === 'SLASHED') {
      actions.add('PARTICIPANT_REPAY')
    } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
      if (opState === 'VALIDATED' && !isOpExpired) {
        if (isValidatorActive) {
          actions.add('OP_RENEW')
        }
        if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
          actions.add('PARTICIPANT_REVOKE')
        }
      } else if (opState === 'PENDING') {
        actions.add('OP_CANCEL')
        if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
          actions.add('PARTICIPANT_REVOKE')
        }
      }
    }
  }

  if (type === 'ECOSYSTEM') {
    if (participantState === 'REPAID' || participantState === 'REVOKED') {
    } else if (participantState === 'SLASHED') {
      actions.add('PARTICIPANT_REPAY')
    } else if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
      actions.add('PARTICIPANT_REVOKE')
      actions.add('PARTICIPANT_ADJUST')
    }
  }

  return Array.from(actions).sort()
}

export const calculateGranteeAvailableActions = calculateCorporationAvailableActions

export function calculateValidatorAvailableActions(
  participant: ParticipantData,
  schema: SchemaData,
  now: Date = new Date()
): ValidatorAction[] {
  const actions: Set<ValidatorAction> = new Set()
  const type = normalizeParticipantType(participant.role)
  const opState = normalizeOpState(participant.op_state)
  const participantState = calculateParticipantState(participant, now)
  const issuerMode = normalizeSchemaMode(schema.issuer_onboarding_mode)
  const verifierMode = normalizeSchemaMode(schema.verifier_onboarding_mode)
  const opExp = participant.op_exp ? new Date(participant.op_exp) : null
  if (isIssuerType(type)) {
    if (issuerMode === 'GRANTOR_VALIDATION' || issuerMode === 'ECOSYSTEM') {
      actions.add('PARTICIPANT_SLASH')

      if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
        actions.add('PARTICIPANT_REVOKE')
        actions.add('PARTICIPANT_ADJUST')
      }

      if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
        if (opState === 'VALIDATED' && opExp && !Number.isNaN(opExp.getTime())) {
        }
      }

      if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        if (opState === 'PENDING') {
          actions.add('OP_SET_VALIDATED')
        }
      }
    }
  }

  if (isVerifierType(type)) {
    const inOpFlow = opState !== null && opState !== 'VALIDATION_STATE_UNSPECIFIED'
    const useOpFlowRules = verifierMode === 'GRANTOR_VALIDATION' || verifierMode === 'ECOSYSTEM' || inOpFlow
    if (useOpFlowRules) {
      actions.add('PARTICIPANT_SLASH')

      if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
        actions.add('PARTICIPANT_REVOKE')
        actions.add('PARTICIPANT_ADJUST')
      }

      if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
        if (opState === 'VALIDATED' && opExp && !Number.isNaN(opExp.getTime())) {
        }
      }

      if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
        if (opState === 'PENDING') {
          actions.add('OP_SET_VALIDATED')
        }
      }
    }
  }

  if (type === 'HOLDER') {
    actions.add('PARTICIPANT_SLASH')

    if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
      actions.add('PARTICIPANT_REVOKE')
      actions.add('PARTICIPANT_ADJUST')
    }

    if (participantState === 'ACTIVE' || participantState === 'FUTURE') {
      if (opState === 'VALIDATED' && opExp && !Number.isNaN(opExp.getTime())) {
      }
    }

    if (participantState === 'ACTIVE' || participantState === 'FUTURE' || participantState === 'INACTIVE') {
      if (opState === 'PENDING') {
        actions.add('OP_SET_VALIDATED')
      }
    }
  }

  return Array.from(actions).sort()
}
