import {
  calculateParticipantState,
  calculateGranteeAvailableActions,
  calculateValidatorAvailableActions,
  pendingFlatMatchesOpPendingWithEligibleParticipantState,
  type ParticipantData,
  type SchemaData,
} from "../../../../src/services/crawl-pp/pp_state_utils";

describe("🧪 pp_state_utils", () => {
  const NOW = new Date("2025-01-10T00:00:00.000Z");

  const baseParticipant: ParticipantData = {
    role: "ISSUER",
  };

  describe("pendingFlatMatchesOpPendingWithEligibleParticipantState", () => {
    it("is true when op_state is PENDING and participant_state is INACTIVE, ACTIVE, FUTURE, or EXPIRED", () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "INACTIVE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "pending", participant_state: "ACTIVE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "FUTURE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "EXPIRED" })
      ).toBe(true);
    });

    it("is false when op_state is PENDING but participant_state is REPAID, REVOKED, or SLASHED", () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "REPAID" })
      ).toBe(false);
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "REVOKED" })
      ).toBe(false);
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "PENDING", participant_state: "SLASHED" })
      ).toBe(false);
    });

    it("is false when op_state is not PENDING", () => {
      expect(
        pendingFlatMatchesOpPendingWithEligibleParticipantState({ op_state: "VALIDATED", participant_state: "ACTIVE" })
      ).toBe(false);
    });
  });

  const makeDate = (iso: string) => new Date(iso).toISOString();

  describe("calculateParticipantState", () => {
    it("returns REPAID when repaid is set", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        repaid: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("REPAID");
    });

    it("returns SLASHED when slashed is set and repaid is not", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        slashed: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("SLASHED");
    });

    it("returns REVOKED when revoked is in the past", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        revoked: makeDate("2025-01-05T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("REVOKED");
    });

    it("returns EXPIRED when effective_until is in the past", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate("2024-12-01T00:00:00.000Z"),
        effective_until: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("EXPIRED");
    });

    it("returns ACTIVE when now is within [effective_from, effective_until]", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("ACTIVE");
    });

    it("returns FUTURE when effective_from is in the future", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        effective_from: makeDate("2025-02-01T00:00:00.000Z"),
      };

      expect(calculateParticipantState(participant, NOW)).toBe("FUTURE");
    });

    it("returns INACTIVE when no timestamps are set", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
      };

      expect(calculateParticipantState(participant, NOW)).toBe("INACTIVE");
    });
  });

  describe("calculateGranteeAvailableActions", () => {
    const openSchema: SchemaData = {
      issuer_onboarding_mode: "OPEN",
      verifier_onboarding_mode: "OPEN",
    };

    it("for ISSUER in OPEN mode and ACTIVE state allows PARTICIPANT_REVOKE and PARTICIPANT_ADJUST", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
      };

      const actions = calculateGranteeAvailableActions(participant, openSchema, null, NOW);

      expect(actions).toEqual(["PARTICIPANT_ADJUST", "PARTICIPANT_REVOKE"]);
    });

    it("for ISSUER in OPEN mode and SLASHED state allows only PARTICIPANT_REPAY", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "ISSUER",
        slashed: makeDate("2025-01-01T00:00:00.000Z"),
      };

      const actions = calculateGranteeAvailableActions(participant, openSchema, null, NOW);

      expect(actions).toEqual(["PARTICIPANT_REPAY"]);
    });

    it("for ISSUER in GRANTOR_VALIDATION with PENDING OP allows OP_CANCEL and PARTICIPANT_REVOKE", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        op_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "GRANTOR_VALIDATION",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(participant, schema, "ACTIVE", NOW);

      expect(actions).toEqual(["OP_CANCEL", "PARTICIPANT_REVOKE"]);
    });

    it("for HOLDER with VALIDATED, non-expired OP and active validator allows OP_RENEW and PARTICIPANT_REVOKE", () => {
      const participant: ParticipantData = {
        role: "HOLDER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        op_state: "VALIDATED",
        op_exp: makeDate("2025-02-01T00:00:00.000Z"),
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "OPEN",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(participant, schema, "ACTIVE", NOW);

      expect(actions).toEqual(["OP_RENEW", "PARTICIPANT_REVOKE"]);
    });

    it("for VERIFIER with OPEN schema, PENDING OP and INACTIVE participant_state allows only OP_CANCEL (issue #63)", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "VERIFIER",
        effective_from: null,
        effective_until: null,
        op_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "OPEN",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(participant, schema, null, NOW);

      expect(actions).toEqual(["OP_CANCEL"]);
    });

    it("for VERIFIER with numeric type (2) and numeric op_state (1) from DB still yields OP_CANCEL", () => {
      const participant = {
        ...baseParticipant,
        role: 2 as unknown as ParticipantData["role"],
        effective_from: null,
        effective_until: null,
        op_state: 1 as unknown as ParticipantData["op_state"],
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "OPEN",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(participant, schema, null, NOW);

      expect(actions).toEqual(["OP_CANCEL"]);
    });
  });

  describe("Issue #63 - client testnet Participant 69 exact payload", () => {
    const openSchema: SchemaData = {
      issuer_onboarding_mode: "OPEN",
      verifier_onboarding_mode: "OPEN",
    };

    it("corporation_available_actions = [OP_CANCEL], validator_available_actions includes OP_SET_VALIDATED", () => {
      const participant69: ParticipantData = {
        role: "VERIFIER",
        repaid: null,
        slashed: null,
        revoked: null,
        effective_from: null,
        effective_until: null,
        op_state: "PENDING",
        op_exp: null,
        validator_participant_id: "1",
      };

      expect(calculateParticipantState(participant69, NOW)).toBe("INACTIVE");

      const granteeActions = calculateGranteeAvailableActions(participant69, openSchema, null, NOW);
      expect(granteeActions).toEqual(["OP_CANCEL"]);

      const validatorActions = calculateValidatorAvailableActions(participant69, openSchema, NOW);
      expect(validatorActions).toContain("OP_SET_VALIDATED");
      expect(validatorActions.sort()).toEqual(["OP_SET_VALIDATED", "PARTICIPANT_SLASH"]);
    });
  });

  describe("calculateValidatorAvailableActions", () => {
    const grantorSchema: SchemaData = {
      issuer_onboarding_mode: "GRANTOR_VALIDATION",
      verifier_onboarding_mode: "GRANTOR_VALIDATION",
    };

    it("for ISSUER in GRANTOR_VALIDATION with ACTIVE state and PENDING OP includes slash, revoke, extend and set validated", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        op_state: "PENDING",
      };

      const actions = calculateValidatorAvailableActions(participant, grantorSchema, NOW);

      expect(actions).toEqual([
        "OP_SET_VALIDATED",
        "PARTICIPANT_ADJUST",
        "PARTICIPANT_REVOKE",
        "PARTICIPANT_SLASH",
      ]);
    });

    it("for HOLDER with ACTIVE state and PENDING OP includes slash, revoke, adjust and set validated", () => {
      const participant: ParticipantData = {
        role: "HOLDER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        op_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "OPEN",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateValidatorAvailableActions(participant, schema, NOW);

      expect(actions).toEqual([
        "OP_SET_VALIDATED",
        "PARTICIPANT_ADJUST",
        "PARTICIPANT_REVOKE",
        "PARTICIPANT_SLASH",
      ]);
    });

    it("for VERIFIER with OPEN schema, PENDING OP and INACTIVE participant_state includes OP_SET_VALIDATED (issue #63)", () => {
      const participant: ParticipantData = {
        ...baseParticipant,
        role: "VERIFIER",
        effective_from: null,
        effective_until: null,
        op_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_onboarding_mode: "OPEN",
        verifier_onboarding_mode: "OPEN",
      };

      const actions = calculateValidatorAvailableActions(participant, schema, NOW);

      expect(actions).toContain("OP_SET_VALIDATED");
      expect(actions.sort()).toEqual(["OP_SET_VALIDATED", "PARTICIPANT_SLASH"]);
    });
  });
});

