import { describe, it, expect } from "@jest/globals";
import {
  ALL_KNOWN_VERANA_MESSAGE_TYPES,
  VeranaCredentialSchemaMessageTypes,
  VeranaParticipantMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaEcosystemMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaCorporationMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
  isKnownVeranaMessageType,
  isVeranaMessageType,
  isEcosystemMessageType,
  isCredentialSchemaMessageType,
  isParticipantMessageType,
  isTrustDepositMessageType,
  isDelegationMessageType,
  isCorporationMessageType,
  isGovernanceFrameworkMessageType,
  isUpdateParamsMessageType,
  getAllKnownMessageTypes,
  shouldSkipUnknownMessages,
} from "../../../src/common/verana-message-types";

describe("verana-message-types", () => {
  describe("Message Type Enums", () => {
    it("should aggregate all indexed Verana and tracked Cosmos message types", () => {
      expect(ALL_KNOWN_VERANA_MESSAGE_TYPES.has(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema)).toBe(true);
      expect(ALL_KNOWN_VERANA_MESSAGE_TYPES.has(VeranaEcosystemMessageTypes.CreateEcosystem)).toBe(true);
      expect(ALL_KNOWN_VERANA_MESSAGE_TYPES.size).toBeGreaterThan(10);
    });

    it("should have correct VeranaCredentialSchemaMessageTypes values", () => {
      expect(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema).toBe("/verana.cs.v1.MsgCreateCredentialSchema");
      expect(VeranaCredentialSchemaMessageTypes.UpdateParams).toBe("/verana.cs.v1.MsgUpdateParams");
      expect(VeranaCredentialSchemaMessageTypes.CreateSchemaAuthorizationPolicy).toBe("/verana.cs.v1.MsgCreateSchemaAuthorizationPolicy");
    });

    it("should have correct VeranaParticipantMessageTypes values", () => {
      expect(VeranaParticipantMessageTypes.CreateRootParticipant).toBe("/verana.pp.v1.MsgCreateRootParticipant");
      expect(VeranaParticipantMessageTypes.UpdateParams).toBe("/verana.pp.v1.MsgUpdateParams");
    });

    it("should have correct VeranaTrustDepositMessageTypes values", () => {
      expect(VeranaTrustDepositMessageTypes.ReclaimYield).toBe("/verana.td.v1.MsgReclaimTrustDepositYield");
      expect(VeranaTrustDepositMessageTypes.UpdateParams).toBe("/verana.td.v1.MsgUpdateParams");
    });

    it("should have correct VeranaEcosystemMessageTypes values", () => {
      expect(VeranaEcosystemMessageTypes.CreateEcosystem).toBe("/verana.ec.v1.MsgCreateEcosystem");
      expect(VeranaEcosystemMessageTypes.UpdateParams).toBe("/verana.ec.v1.MsgUpdateParams");
    });

    it("should have correct VeranaDelegationMessageTypes values", () => {
      expect(VeranaDelegationMessageTypes.GrantOperatorAuthorization).toBe("/verana.de.v1.MsgGrantOperatorAuthorization");
      expect(VeranaDelegationMessageTypes.RevokeOperatorAuthorization).toBe("/verana.de.v1.MsgRevokeOperatorAuthorization");
      expect(VeranaDelegationMessageTypes.UpdateParams).toBe("/verana.de.v1.MsgUpdateParams");
    });

    it("should have correct VeranaCorporationMessageTypes values", () => {
      expect(VeranaCorporationMessageTypes.CreateCorporation).toBe("/verana.co.v1.MsgCreateCorporation");
      expect(VeranaCorporationMessageTypes.UpdateCorporation).toBe("/verana.co.v1.MsgUpdateCorporation");
      expect(VeranaCorporationMessageTypes.UpdateParams).toBe("/verana.co.v1.MsgUpdateParams");
    });

    it("should have correct VeranaGovernanceFrameworkMessageTypes values", () => {
      expect(VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument).toBe("/verana.gf.v1.MsgAddGovernanceFrameworkDocument");
      expect(VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion).toBe("/verana.gf.v1.MsgIncreaseActiveGovernanceFrameworkVersion");
      expect(VeranaGovernanceFrameworkMessageTypes.UpdateParams).toBe("/verana.gf.v1.MsgUpdateParams");
    });

    it("should register Corporation and GovernanceFramework types as known", () => {
      expect(isKnownVeranaMessageType(VeranaCorporationMessageTypes.CreateCorporation)).toBe(true);
      expect(isKnownVeranaMessageType(VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument)).toBe(true);
    });
  });

  describe("isVeranaMessageType", () => {
    it("should return true for Verana message types", () => {
      expect(isVeranaMessageType("/verana.cs.v1.MsgCreateCredentialSchema")).toBe(true);
      expect(isVeranaMessageType("/verana.ec.v1.MsgCreateEcosystem")).toBe(true);
    });

    it("should return false for non-Verana message types", () => {
      expect(isVeranaMessageType("/cosmos.staking.v1beta1.MsgDelegate")).toBe(false);
      expect(isVeranaMessageType("invalid")).toBe(false);
    });
  });

  describe("isKnownVeranaMessageType", () => {
    it("should return true for known Verana message types", () => {
      expect(isKnownVeranaMessageType(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema)).toBe(true);
      expect(isKnownVeranaMessageType(VeranaParticipantMessageTypes.CreateRootParticipant)).toBe(true);
      expect(isKnownVeranaMessageType(VeranaDelegationMessageTypes.GrantOperatorAuthorization)).toBe(true);
    });

    it("should return false for unknown message types", () => {
      expect(isKnownVeranaMessageType("/verana.unknown.v1.MsgUnknown")).toBe(false);
    });

    it("should return true for Cosmos message types that are tracked", () => {
      expect(isKnownVeranaMessageType("/cosmos.staking.v1beta1.MsgDelegate")).toBe(true);
    });
  });

  describe("Message Type Checkers", () => {
    it("isEcosystemMessageType should work correctly", () => {
      expect(isEcosystemMessageType(VeranaEcosystemMessageTypes.CreateEcosystem)).toBe(true);
      expect(isEcosystemMessageType(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema)).toBe(false);
    });

    it("isCredentialSchemaMessageType should work correctly", () => {
      expect(isCredentialSchemaMessageType(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema)).toBe(true);
      expect(isCredentialSchemaMessageType(VeranaEcosystemMessageTypes.CreateEcosystem)).toBe(false);
    });

    it("isParticipantMessageType should work correctly", () => {
      expect(isParticipantMessageType(VeranaParticipantMessageTypes.CreateRootParticipant)).toBe(true);
      expect(isParticipantMessageType(VeranaEcosystemMessageTypes.CreateEcosystem)).toBe(false);
    });

    it("isTrustDepositMessageType should work correctly", () => {
      expect(isTrustDepositMessageType(VeranaTrustDepositMessageTypes.ReclaimYield)).toBe(true);
      expect(isTrustDepositMessageType(VeranaParticipantMessageTypes.CreateRootParticipant)).toBe(false);
    });

    it("isDelegationMessageType should work correctly", () => {
      expect(isDelegationMessageType(VeranaDelegationMessageTypes.GrantOperatorAuthorization)).toBe(true);
      expect(isDelegationMessageType(VeranaParticipantMessageTypes.CreateRootParticipant)).toBe(false);
    });

    it("isCorporationMessageType should work correctly", () => {
      expect(isCorporationMessageType(VeranaCorporationMessageTypes.CreateCorporation)).toBe(true);
      expect(isCorporationMessageType(VeranaEcosystemMessageTypes.CreateEcosystem)).toBe(false);
    });

    it("isGovernanceFrameworkMessageType should work correctly", () => {
      expect(isGovernanceFrameworkMessageType(VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument)).toBe(true);
      expect(isGovernanceFrameworkMessageType(VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc)).toBe(false);
    });

    it("isUpdateParamsMessageType should work correctly", () => {
      expect(isUpdateParamsMessageType("/verana.unknown.v1.MsgUpdateParams")).toBe(false);
      expect(isUpdateParamsMessageType(VeranaCredentialSchemaMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaParticipantMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaTrustDepositMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaEcosystemMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaCorporationMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaGovernanceFrameworkMessageTypes.UpdateParams)).toBe(true);
    });
  });

  describe("getAllKnownMessageTypes", () => {
    it("should return array of all known message types", () => {
      const types = getAllKnownMessageTypes();
      expect(Array.isArray(types)).toBe(true);
      expect(types.length).toBeGreaterThan(0);
      expect(types).toContain(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema);
      expect(types).toContain(VeranaParticipantMessageTypes.CreateRootParticipant);
    });
  });

  describe("shouldSkipUnknownMessages", () => {
    it("should return false by default", () => {
      delete process.env.SKIP_UNKNOWN_MESSAGES;
      expect(shouldSkipUnknownMessages()).toBe(false);
    });

    it("should return true when SKIP_UNKNOWN_MESSAGES is 'true'", () => {
      process.env.SKIP_UNKNOWN_MESSAGES = "true";
      expect(shouldSkipUnknownMessages()).toBe(true);
      delete process.env.SKIP_UNKNOWN_MESSAGES;
    });

    it("should return true when SKIP_UNKNOWN_MESSAGES is '1'", () => {
      process.env.SKIP_UNKNOWN_MESSAGES = "1";
      expect(shouldSkipUnknownMessages()).toBe(true);
      delete process.env.SKIP_UNKNOWN_MESSAGES;
    });
  });
});
