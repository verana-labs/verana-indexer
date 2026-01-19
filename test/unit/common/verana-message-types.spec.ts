import { describe, it, expect } from "@jest/globals";
import {
  VeranaDidMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaTrustRegistryMessageTypes,
  isKnownVeranaMessageType,
  isVeranaMessageType,
  isDidMessageType,
  isTrustRegistryMessageType,
  isCredentialSchemaMessageType,
  isPermissionMessageType,
  isTrustDepositMessageType,
  isUpdateParamsMessageType,
  getAllKnownMessageTypes,
  shouldSkipUnknownMessages,
} from "../../../src/common/verana-message-types";

describe("verana-message-types", () => {
  describe("Message Type Enums", () => {
    it("should have correct VeranaDidMessageTypes values", () => {
      expect(VeranaDidMessageTypes.AddDid).toBe("/verana.dd.v1.MsgAddDID");
      expect(VeranaDidMessageTypes.UpdateParams).toBe("/verana.dd.v1.MsgUpdateParams");
    });

    it("should have correct VeranaCredentialSchemaMessageTypes values", () => {
      expect(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema).toBe("/verana.cs.v1.MsgCreateCredentialSchema");
      expect(VeranaCredentialSchemaMessageTypes.UpdateParams).toBe("/verana.cs.v1.MsgUpdateParams");
    });

    it("should have correct VeranaPermissionMessageTypes values", () => {
      expect(VeranaPermissionMessageTypes.CreateRootPermission).toBe("/verana.perm.v1.MsgCreateRootPermission");
      expect(VeranaPermissionMessageTypes.UpdateParams).toBe("/verana.perm.v1.MsgUpdateParams");
    });

    it("should have correct VeranaTrustDepositMessageTypes values", () => {
      expect(VeranaTrustDepositMessageTypes.ReclaimYield).toBe("/verana.td.v1.MsgReclaimTrustDepositYield");
      expect(VeranaTrustDepositMessageTypes.UpdateParams).toBe("/verana.td.v1.MsgUpdateParams");
    });

    it("should have correct VeranaTrustRegistryMessageTypes values", () => {
      expect(VeranaTrustRegistryMessageTypes.CreateTrustRegistry).toBe("/verana.tr.v1.MsgCreateTrustRegistry");
      expect(VeranaTrustRegistryMessageTypes.UpdateParams).toBe("/verana.tr.v1.MsgUpdateParams");
    });
  });

  describe("isVeranaMessageType", () => {
    it("should return true for Verana message types", () => {
      expect(isVeranaMessageType("/verana.dd.v1.MsgAddDID")).toBe(true);
      expect(isVeranaMessageType("/verana.cs.v1.MsgCreateCredentialSchema")).toBe(true);
    });

    it("should return false for non-Verana message types", () => {
      expect(isVeranaMessageType("/cosmos.staking.v1beta1.MsgDelegate")).toBe(false);
      expect(isVeranaMessageType("invalid")).toBe(false);
    });
  });

  describe("isKnownVeranaMessageType", () => {
    it("should return true for known Verana message types", () => {
      expect(isKnownVeranaMessageType(VeranaDidMessageTypes.AddDid)).toBe(true);
      expect(isKnownVeranaMessageType(VeranaPermissionMessageTypes.CreateRootPermission)).toBe(true);
    });

    it("should return false for unknown message types", () => {
      expect(isKnownVeranaMessageType("/verana.unknown.v1.MsgUnknown")).toBe(false);
    });

    it("should return true for Cosmos message types that are tracked", () => {
      expect(isKnownVeranaMessageType("/cosmos.staking.v1beta1.MsgDelegate")).toBe(true);
    });
  });

  describe("Message Type Checkers", () => {
    it("isDidMessageType should work correctly", () => {
      expect(isDidMessageType(VeranaDidMessageTypes.AddDid)).toBe(true);
      expect(isDidMessageType(VeranaDidMessageTypes.UpdateParams)).toBe(true);
      expect(isDidMessageType(VeranaPermissionMessageTypes.CreateRootPermission)).toBe(false);
    });

    it("isTrustRegistryMessageType should work correctly", () => {
      expect(isTrustRegistryMessageType(VeranaTrustRegistryMessageTypes.CreateTrustRegistry)).toBe(true);
      expect(isTrustRegistryMessageType(VeranaDidMessageTypes.AddDid)).toBe(false);
    });

    it("isCredentialSchemaMessageType should work correctly", () => {
      expect(isCredentialSchemaMessageType(VeranaCredentialSchemaMessageTypes.CreateCredentialSchema)).toBe(true);
      expect(isCredentialSchemaMessageType(VeranaDidMessageTypes.AddDid)).toBe(false);
    });

    it("isPermissionMessageType should work correctly", () => {
      expect(isPermissionMessageType(VeranaPermissionMessageTypes.CreateRootPermission)).toBe(true);
      expect(isPermissionMessageType(VeranaDidMessageTypes.AddDid)).toBe(false);
    });

    it("isTrustDepositMessageType should work correctly", () => {
      expect(isTrustDepositMessageType(VeranaTrustDepositMessageTypes.ReclaimYield)).toBe(true);
      expect(isTrustDepositMessageType(VeranaDidMessageTypes.AddDid)).toBe(false);
    });

    it("isUpdateParamsMessageType should work correctly", () => {
      expect(isUpdateParamsMessageType(VeranaDidMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaCredentialSchemaMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaPermissionMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaTrustDepositMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaTrustRegistryMessageTypes.UpdateParams)).toBe(true);
      expect(isUpdateParamsMessageType(VeranaDidMessageTypes.AddDid)).toBe(false);
    });
  });

  describe("getAllKnownMessageTypes", () => {
    it("should return array of all known message types", () => {
      const types = getAllKnownMessageTypes();
      expect(Array.isArray(types)).toBe(true);
      expect(types.length).toBeGreaterThan(0);
      expect(types).toContain(VeranaDidMessageTypes.AddDid);
      expect(types).toContain(VeranaPermissionMessageTypes.CreateRootPermission);
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

