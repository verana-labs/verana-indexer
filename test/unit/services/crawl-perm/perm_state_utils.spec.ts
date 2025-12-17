import {
  calculatePermState,
  calculateGranteeAvailableActions,
  calculateValidatorAvailableActions,
  type PermissionData,
  type SchemaData,
} from "../../../../src/services/crawl-perm/perm_state_utils";

describe("🧪 perm_state_utils", () => {
  const NOW = new Date("2025-01-10T00:00:00.000Z");

  const basePerm: PermissionData = {
    type: "ISSUER",
  };

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
  });
});

