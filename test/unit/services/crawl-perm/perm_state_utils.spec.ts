import {
  calculatePermState,
  calculateGranteeAvailableActions,
  calculateValidatorAvailableActions,
  pendingFlatMatchesVpPendingWithEligiblePermState,
  type PermissionData,
  type SchemaData,
} from "../../../../src/services/crawl-perm/perm_state_utils";

describe("🧪 perm_state_utils", () => {
  const NOW = new Date("2025-01-10T00:00:00.000Z");

  const basePerm: PermissionData = {
    type: "ISSUER",
  };

  describe("pendingFlatMatchesVpPendingWithEligiblePermState", () => {
    it("is true when vp_state is PENDING and perm_state is INACTIVE, ACTIVE, FUTURE, or EXPIRED", () => {
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "INACTIVE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "pending", perm_state: "ACTIVE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "FUTURE" })
      ).toBe(true);
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "EXPIRED" })
      ).toBe(true);
    });

    it("is false when vp_state is PENDING but perm_state is REPAID, REVOKED, or SLASHED", () => {
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "REPAID" })
      ).toBe(false);
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "REVOKED" })
      ).toBe(false);
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "PENDING", perm_state: "SLASHED" })
      ).toBe(false);
    });

    it("is false when vp_state is not PENDING", () => {
      expect(
        pendingFlatMatchesVpPendingWithEligiblePermState({ vp_state: "VALIDATED", perm_state: "ACTIVE" })
      ).toBe(false);
    });
  });

  const makeDate = (iso: string) => new Date(iso).toISOString();

  describe("calculatePermState", () => {
    it("returns REPAID when repaid is set", () => {
      const perm: PermissionData = {
        ...basePerm,
        repaid: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("REPAID");
    });

    it("returns SLASHED when slashed is set and repaid is not", () => {
      const perm: PermissionData = {
        ...basePerm,
        slashed: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("SLASHED");
    });

    it("returns REVOKED when revoked is in the past", () => {
      const perm: PermissionData = {
        ...basePerm,
        revoked: makeDate("2025-01-05T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("REVOKED");
    });

    it("returns EXPIRED when effective_until is in the past", () => {
      const perm: PermissionData = {
        ...basePerm,
        effective_from: makeDate("2024-12-01T00:00:00.000Z"),
        effective_until: makeDate("2025-01-01T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("EXPIRED");
    });

    it("returns ACTIVE when now is within [effective_from, effective_until]", () => {
      const perm: PermissionData = {
        ...basePerm,
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("ACTIVE");
    });

    it("returns FUTURE when effective_from is in the future", () => {
      const perm: PermissionData = {
        ...basePerm,
        effective_from: makeDate("2025-02-01T00:00:00.000Z"),
      };

      expect(calculatePermState(perm, NOW)).toBe("FUTURE");
    });

    it("returns INACTIVE when no timestamps are set", () => {
      const perm: PermissionData = {
        ...basePerm,
      };

      expect(calculatePermState(perm, NOW)).toBe("INACTIVE");
    });
  });

  describe("calculateGranteeAvailableActions", () => {
    const openSchema: SchemaData = {
      issuer_perm_management_mode: "OPEN",
      verifier_perm_management_mode: "OPEN",
    };

    it("for ISSUER in OPEN mode and ACTIVE state allows PERM_REVOKE and PERM_EXTEND", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
      };

      const actions = calculateGranteeAvailableActions(perm, openSchema, null, NOW);

      expect(actions).toEqual(["PERM_EXTEND", "PERM_REVOKE"]);
    });

    it("for ISSUER in OPEN mode and SLASHED state allows only PERM_REPAY", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "ISSUER",
        slashed: makeDate("2025-01-01T00:00:00.000Z"),
      };

      const actions = calculateGranteeAvailableActions(perm, openSchema, null, NOW);

      expect(actions).toEqual(["PERM_REPAY"]);
    });

    it("for ISSUER in GRANTOR_VALIDATION with PENDING VP allows VP_CANCEL and PERM_REVOKE", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        vp_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "GRANTOR_VALIDATION",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(perm, schema, "ACTIVE", NOW);

      expect(actions).toEqual(["PERM_REVOKE", "VP_CANCEL"]);
    });

    it("for HOLDER with VALIDATED, non-expired VP and active validator allows VP_RENEW and PERM_REVOKE", () => {
      const perm: PermissionData = {
        type: "HOLDER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        vp_state: "VALIDATED",
        vp_exp: makeDate("2025-02-01T00:00:00.000Z"),
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "OPEN",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(perm, schema, "ACTIVE", NOW);

      expect(actions).toEqual(["PERM_REVOKE", "VP_RENEW"]);
    });

    it("for VERIFIER with OPEN schema, PENDING VP and INACTIVE perm_state allows only VP_CANCEL (issue #63)", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "VERIFIER",
        effective_from: null,
        effective_until: null,
        vp_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "OPEN",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(perm, schema, null, NOW);

      expect(actions).toEqual(["VP_CANCEL"]);
    });

    it("for VERIFIER with numeric type (2) and numeric vp_state (1) from DB still yields VP_CANCEL", () => {
      const perm = {
        ...basePerm,
        type: 2 as unknown as PermissionData["type"],
        effective_from: null,
        effective_until: null,
        vp_state: 1 as unknown as PermissionData["vp_state"],
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "OPEN",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateGranteeAvailableActions(perm, schema, null, NOW);

      expect(actions).toEqual(["VP_CANCEL"]);
    });
  });

  describe("Issue #63 - client testnet Permission 69 exact payload", () => {
    const openSchema: SchemaData = {
      issuer_perm_management_mode: "OPEN",
      verifier_perm_management_mode: "OPEN",
    };

    it("grantee_available_actions = [VP_CANCEL], validator_available_actions includes VP_SET_VALIDATED", () => {
      const perm69: PermissionData = {
        type: "VERIFIER",
        repaid: null,
        slashed: null,
        revoked: null,
        effective_from: null,
        effective_until: null,
        vp_state: "PENDING",
        vp_exp: null,
        validator_perm_id: "1",
      };

      expect(calculatePermState(perm69, NOW)).toBe("INACTIVE");

      const granteeActions = calculateGranteeAvailableActions(perm69, openSchema, null, NOW);
      expect(granteeActions).toEqual(["VP_CANCEL"]);

      const validatorActions = calculateValidatorAvailableActions(perm69, openSchema, NOW);
      expect(validatorActions).toContain("VP_SET_VALIDATED");
      expect(validatorActions.sort()).toEqual(["PERM_SLASH", "VP_SET_VALIDATED"]);
    });
  });

  describe("calculateValidatorAvailableActions", () => {
    const grantorSchema: SchemaData = {
      issuer_perm_management_mode: "GRANTOR_VALIDATION",
      verifier_perm_management_mode: "GRANTOR_VALIDATION",
    };

    it("for ISSUER in GRANTOR_VALIDATION with ACTIVE state and PENDING VP includes slash, revoke, extend and set validated", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "ISSUER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        vp_state: "PENDING",
      };

      const actions = calculateValidatorAvailableActions(perm, grantorSchema, NOW);

      expect(actions).toEqual([
        "PERM_EXTEND",
        "PERM_REVOKE",
        "PERM_SLASH",
        "VP_SET_VALIDATED",
      ]);
    });

    it("for HOLDER with ACTIVE state and PENDING VP includes slash, revoke, extend and set validated", () => {
      const perm: PermissionData = {
        type: "HOLDER",
        effective_from: makeDate("2025-01-01T00:00:00.000Z"),
        effective_until: makeDate("2025-02-01T00:00:00.000Z"),
        vp_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "OPEN",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateValidatorAvailableActions(perm, schema, NOW);

      expect(actions).toEqual([
        "PERM_EXTEND",
        "PERM_REVOKE",
        "PERM_SLASH",
        "VP_SET_VALIDATED",
      ]);
    });

    it("for VERIFIER with OPEN schema, PENDING VP and INACTIVE perm_state includes VP_SET_VALIDATED (issue #63)", () => {
      const perm: PermissionData = {
        ...basePerm,
        type: "VERIFIER",
        effective_from: null,
        effective_until: null,
        vp_state: "PENDING",
      };

      const schema: SchemaData = {
        issuer_perm_management_mode: "OPEN",
        verifier_perm_management_mode: "OPEN",
      };

      const actions = calculateValidatorAvailableActions(perm, schema, NOW);

      expect(actions).toContain("VP_SET_VALIDATED");
      expect(actions.sort()).toEqual(["PERM_SLASH", "VP_SET_VALIDATED"]);
    });
  });
});

