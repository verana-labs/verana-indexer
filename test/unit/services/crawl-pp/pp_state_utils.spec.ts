import {
  calculateGranteeAvailableActions,
  calculateParticipantState,
  calculateValidatorAvailableActions,
  type ParticipantData,
  pendingFlatMatchesOpPendingWithEligibleParticipantState,
  type SchemaData,
} from '../../../../src/services/crawl-pp/pp_state_utils'

describe('🧪 pp_state_utils', () => {
  const NOW = new Date('2025-01-10T00:00:00.000Z')

  const baseParticipant: ParticipantData = {
    role: 'ISSUER',
  }

  describe('pendingFlatMatchesOpPendingWithEligibleParticipantState', () => {
    it('is true when op_state is PENDING and participant_state is INACTIVE, ACTIVE, FUTURE, or EXPIRED', () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'INACTIVE' })
      ).toBe(true)
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'pending', participant_state: 'ACTIVE' })
      ).toBe(true)
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'FUTURE' })
      ).toBe(true)
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'EXPIRED' })
      ).toBe(true)
    })

    it('is false when op_state is PENDING but participant_state is REPAID, REVOKED, or SLASHED', () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'REPAID' })
      ).toBe(false)
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'REVOKED' })
      ).toBe(false)
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'PENDING', participant_state: 'SLASHED' })
      ).toBe(false)
    })

    it('is false when op_state is not PENDING', () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: 'VALIDATED', participant_state: 'ACTIVE' })
      ).toBe(false)
    })
  })

  const makeDate = (iso: string) => new Date(iso).toISOString()

  describe('calculateParticipantState', () => {
    it('returns REPAID when repaid is at or after slashed', () => {
      const slashed = makeDate('2025-01-01T00:00:00.000Z')
      expect(calculateParticipantState({ ...baseParticipant, slashed, repaid: slashed }, NOW)).toBe('REPAID')
      expect(
        calculateParticipantState({ ...baseParticipant, slashed, repaid: makeDate('2025-01-02T00:00:00.000Z') }, NOW)
      ).toBe('REPAID')
    })

    it('returns SLASHED when slashed again after a repayment', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        slashed: makeDate('2025-01-05T00:00:00.000Z'),
        repaid: makeDate('2025-01-02T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('SLASHED')
    })

    it('ignores repaid when the entry was never slashed', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        repaid: makeDate('2025-01-01T00:00:00.000Z'),
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('ACTIVE')
    })

    it('returns SLASHED when slashed is set and repaid is not', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        slashed: makeDate('2025-01-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('SLASHED')
    })

    it('returns REVOKED when revoked is in the past', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        revoked: makeDate('2025-01-05T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('REVOKED')
    })

    it('returns REVOKED when revoked equals the evaluation instant', () => {
      expect(calculateParticipantState({ ...baseParticipant, revoked: NOW.toISOString() }, NOW)).toBe('REVOKED')
    })

    it('ignores a revoked timestamp that is still in the future', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        revoked: makeDate('2025-01-11T00:00:00.000Z'),
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('ACTIVE')
    })

    it('returns EXPIRED when effective_until is in the past', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate('2024-12-01T00:00:00.000Z'),
        effective_until: makeDate('2025-01-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('EXPIRED')
    })

    it('returns EXPIRED when effective_until equals the evaluation instant', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate('2024-12-01T00:00:00.000Z'),
        effective_until: NOW.toISOString(),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('EXPIRED')
    })

    it('returns ACTIVE when effective_from equals the evaluation instant', () => {
      expect(calculateParticipantState({ ...baseParticipant, effective_from: NOW.toISOString() }, NOW)).toBe('ACTIVE')
    })

    it('ranks SLASHED above REVOKED and EXPIRED, and EXPIRED above FUTURE', () => {
      const past = makeDate('2025-01-01T00:00:00.000Z')
      const future = makeDate('2025-02-01T00:00:00.000Z')
      expect(
        calculateParticipantState({ ...baseParticipant, slashed: past, revoked: past, effective_until: past }, NOW)
      ).toBe('SLASHED')
      expect(
        calculateParticipantState(
          { ...baseParticipant, revoked: past, effective_until: past, effective_from: past },
          NOW
        )
      ).toBe('REVOKED')
      expect(
        calculateParticipantState({ ...baseParticipant, effective_from: future, effective_until: past }, NOW)
      ).toBe('EXPIRED')
    })

    it('returns ACTIVE when now is within [effective_from, effective_until]', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('ACTIVE')
    })

    it('returns FUTURE when effective_from is in the future', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate('2025-02-01T00:00:00.000Z'),
      }

      expect(calculateParticipantState(participant, NOW)).toBe('FUTURE')
    })

    it('returns ACTIVE when effective_from is null and no onboarding process is pending', () => {
      expect(calculateParticipantState({ ...baseParticipant }, NOW)).toBe('ACTIVE')
      expect(calculateParticipantState({ ...baseParticipant, op_state: null }, NOW)).toBe('ACTIVE')
      expect(calculateParticipantState({ ...baseParticipant, op_state: 'VALIDATED' }, NOW)).toBe('ACTIVE')
      expect(calculateParticipantState({ ...baseParticipant, op_state: 'VALIDATION_STATE_UNSPECIFIED' }, NOW)).toBe(
        'ACTIVE'
      )
    })

    it('returns INACTIVE when effective_from is null and the onboarding process is PENDING or TERMINATED', () => {
      expect(calculateParticipantState({ ...baseParticipant, op_state: 'PENDING' }, NOW)).toBe('INACTIVE')
      expect(calculateParticipantState({ ...baseParticipant, op_state: 'TERMINATED' }, NOW)).toBe('INACTIVE')
    })
  })

  describe('calculateGranteeAvailableActions', () => {
    const openSchema: SchemaData = {
      issuer_onboarding_mode: 'OPEN',
      verifier_onboarding_mode: 'OPEN',
    }

    it('for ISSUER in OPEN mode and ACTIVE state allows PARTICIPANT_REVOKE and PARTICIPANT_ADJUST', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'ISSUER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
      }

      const actions = calculateGranteeAvailableActions(participant, openSchema, null, NOW)

      expect(actions).toEqual(['PARTICIPANT_ADJUST', 'PARTICIPANT_REVOKE'])
    })

    it('allows only PARTICIPANT_REPAY in SLASHED state, for every role and whatever the validator', () => {
      const slashed = { slashed: makeDate('2025-01-01T00:00:00.000Z') }

      expect(
        calculateGranteeAvailableActions({ ...baseParticipant, role: 'ISSUER', ...slashed }, openSchema, null, NOW)
      ).toEqual(['PARTICIPANT_REPAY'])
      expect(calculateGranteeAvailableActions({ role: 'ISSUER_GRANTOR', ...slashed }, openSchema, null, NOW)).toEqual([
        'PARTICIPANT_REPAY',
      ])
      expect(calculateGranteeAvailableActions({ role: 'VERIFIER_GRANTOR', ...slashed }, openSchema, null, NOW)).toEqual(
        ['PARTICIPANT_REPAY']
      )
      expect(
        calculateGranteeAvailableActions(
          { role: 'ECOSYSTEM', validator_participant_id: '1', ...slashed },
          openSchema,
          null,
          NOW
        )
      ).toEqual(['PARTICIPANT_REPAY'])
    })

    it('offers PARTICIPANT_REPAY again when the entry is slashed after a repayment', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'ISSUER',
        slashed: makeDate('2025-01-05T00:00:00.000Z'),
        repaid: makeDate('2025-01-02T00:00:00.000Z'),
      }

      expect(calculateGranteeAvailableActions(participant, openSchema, null, NOW)).toEqual(['PARTICIPANT_REPAY'])
    })

    it('keeps OP_RENEW and PARTICIPANT_REVOKE for a VALIDATED entry whose op_exp has passed', () => {
      const participant: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        op_exp: makeDate('2025-01-05T00:00:00.000Z'),
        validator_participant_id: '1',
      }

      const actions = calculateGranteeAvailableActions(participant, openSchema, 'ACTIVE', NOW)

      expect(actions).toEqual(['OP_RENEW', 'PARTICIPANT_REVOKE'])
    })

    it('for ISSUER in GRANTOR_VALIDATION with PENDING OP allows OP_CANCEL and PARTICIPANT_REVOKE', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'ISSUER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'PENDING',
        validator_participant_id: '1',
      }

      const schema: SchemaData = {
        issuer_onboarding_mode: 'GRANTOR_ONBOARDING_PROCESS',
        verifier_onboarding_mode: 'OPEN',
      }

      const actions = calculateGranteeAvailableActions(participant, schema, 'ACTIVE', NOW)

      expect(actions).toEqual(['OP_CANCEL', 'PARTICIPANT_REVOKE'])
    })

    it('for HOLDER with VALIDATED, non-expired OP and active validator allows OP_RENEW and PARTICIPANT_REVOKE', () => {
      const participant: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        op_exp: makeDate('2025-02-01T00:00:00.000Z'),
        validator_participant_id: '1',
      }

      const actions = calculateGranteeAvailableActions(participant, openSchema, 'ACTIVE', NOW)

      expect(actions).toEqual(['OP_RENEW', 'PARTICIPANT_REVOKE'])
    })

    it('for VERIFIER with OPEN schema, PENDING OP and INACTIVE participant_state allows only OP_CANCEL (issue #63)', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'VERIFIER',
        effective_from: null,
        effective_until: null,
        op_state: 'PENDING',
        validator_participant_id: '1',
      }

      const actions = calculateGranteeAvailableActions(participant, openSchema, null, NOW)

      expect(actions).toEqual(['OP_CANCEL'])
    })

    it('for a self-managed VERIFIER on an OPEN schema allows revoke and adjust, and no validator action (issue #468)', () => {
      const participant: ParticipantData = {
        role: 'VERIFIER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        validator_participant_id: null,
      }

      expect(calculateGranteeAvailableActions(participant, openSchema, null, NOW)).toEqual([
        'PARTICIPANT_ADJUST',
        'PARTICIPANT_REVOKE',
      ])
      expect(calculateValidatorAvailableActions(participant, NOW)).toEqual([])
    })

    it('for ECOSYSTEM keeps revoke and adjust even when the entry carries a validator', () => {
      const participant: ParticipantData = {
        role: 'ECOSYSTEM',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        validator_participant_id: '1',
      }

      expect(calculateGranteeAvailableActions(participant, openSchema, null, NOW)).toEqual([
        'PARTICIPANT_ADJUST',
        'PARTICIPANT_REVOKE',
      ])
    })

    it('for VERIFIER with numeric type (2) and numeric op_state (1) from DB still yields OP_CANCEL', () => {
      const participant = {
        ...baseParticipant,
        role: 2 as unknown as ParticipantData['role'],
        effective_from: null,
        effective_until: null,
        op_state: 1 as unknown as ParticipantData['op_state'],
        validator_participant_id: '1',
      }

      const actions = calculateGranteeAvailableActions(participant, openSchema, null, NOW)

      expect(actions).toEqual(['OP_CANCEL'])
    })
  })

  describe('Issue #63 - client testnet Participant 69 exact payload', () => {
    const openSchema: SchemaData = {
      issuer_onboarding_mode: 'OPEN',
      verifier_onboarding_mode: 'OPEN',
    }

    it('corporation_available_actions = [OP_CANCEL], validator_available_actions includes OP_SET_VALIDATED', () => {
      const participant69: ParticipantData = {
        role: 'VERIFIER',
        repaid: null,
        slashed: null,
        revoked: null,
        effective_from: null,
        effective_until: null,
        op_state: 'PENDING',
        op_exp: null,
        validator_participant_id: '1',
      }

      expect(calculateParticipantState(participant69, NOW)).toBe('INACTIVE')

      const granteeActions = calculateGranteeAvailableActions(participant69, openSchema, null, NOW)
      expect(granteeActions).toEqual(['OP_CANCEL'])

      const validatorActions = calculateValidatorAvailableActions(participant69, NOW)
      expect(validatorActions).toContain('OP_SET_VALIDATED')
      expect(validatorActions.sort()).toEqual(['OP_SET_VALIDATED', 'PARTICIPANT_SLASH'])
    })
  })

  describe('calculateValidatorAvailableActions', () => {
    it('for ISSUER mid renewal (ACTIVE, PENDING OP) includes slash, revoke, adjust and set validated', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'ISSUER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'PENDING',
        validator_participant_id: '1',
      }

      const actions = calculateValidatorAvailableActions(participant, NOW)

      expect(actions).toEqual(['OP_SET_VALIDATED', 'PARTICIPANT_ADJUST', 'PARTICIPANT_REVOKE', 'PARTICIPANT_SLASH'])
    })

    it('for ISSUER with ACTIVE state and VALIDATED OP includes slash, revoke and adjust', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'ISSUER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        op_exp: makeDate('2025-03-01T00:00:00.000Z'),
        validator_participant_id: '1',
      }

      const actions = calculateValidatorAvailableActions(participant, NOW)

      expect(actions).toEqual(['PARTICIPANT_ADJUST', 'PARTICIPANT_REVOKE', 'PARTICIPANT_SLASH'])
    })

    it('for HOLDER mid renewal (ACTIVE, PENDING OP) includes slash, revoke, adjust and set validated', () => {
      const participant: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'PENDING',
        validator_participant_id: '1',
      }

      const actions = calculateValidatorAvailableActions(participant, NOW)

      expect(actions).toEqual(['OP_SET_VALIDATED', 'PARTICIPANT_ADJUST', 'PARTICIPANT_REVOKE', 'PARTICIPANT_SLASH'])
    })

    it('for HOLDER with ACTIVE state and VALIDATED OP includes slash, revoke and adjust', () => {
      const participant: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        effective_until: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        validator_participant_id: '1',
      }

      const actions = calculateValidatorAvailableActions(participant, NOW)

      expect(actions).toEqual(['PARTICIPANT_ADJUST', 'PARTICIPANT_REVOKE', 'PARTICIPANT_SLASH'])
    })

    it('for a FUTURE entry offers only slash, since revoke and adjust need an active participant', () => {
      const future: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-02-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        validator_participant_id: '1',
      }

      expect(calculateValidatorAvailableActions(future, NOW)).toEqual(['PARTICIPANT_SLASH'])
      expect(calculateValidatorAvailableActions({ ...future, role: 'ISSUER' }, NOW)).toEqual(['PARTICIPANT_SLASH'])
    })

    it('offers nothing when the entry is self-managed, since no validator Corporation owns it', () => {
      const selfManaged: ParticipantData = {
        role: 'HOLDER',
        effective_from: makeDate('2025-01-01T00:00:00.000Z'),
        op_state: 'VALIDATED',
        validator_participant_id: null,
      }

      expect(calculateValidatorAvailableActions(selfManaged, NOW)).toEqual([])
      expect(calculateValidatorAvailableActions({ ...selfManaged, role: 'ISSUER' }, NOW)).toEqual([])
    })

    it('for VERIFIER with PENDING OP and INACTIVE participant_state includes OP_SET_VALIDATED (issue #63)', () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: 'VERIFIER',
        effective_from: null,
        effective_until: null,
        op_state: 'PENDING',
        validator_participant_id: '1',
      }

      const actions = calculateValidatorAvailableActions(participant, NOW)

      expect(actions).toContain('OP_SET_VALIDATED')
      expect(actions.sort()).toEqual(['OP_SET_VALIDATED', 'PARTICIPANT_SLASH'])
    })
  })
})
