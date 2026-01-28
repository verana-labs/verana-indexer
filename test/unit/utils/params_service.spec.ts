import { describe, it, expect, beforeEach, afterEach } from "@jest/globals";
import {
  getModuleParams,
  getModuleParamsAction,
  parseModuleParams,
  clearParamsCache,
} from "../../../src/common/utils/params_service";
import knex from "../../../src/common/utils/db_connection";
import ModuleParams from "../../../src/models/modules_params";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES } from "../../../src/common/constant";
import { Context } from "moleculer";

jest.mock("../../../src/common/utils/db_connection");
jest.mock("../../../src/models/modules_params");

describe("params_service", () => {
  beforeEach(() => {
    clearParamsCache();
    jest.clearAllMocks();
  });

  afterEach(() => {
    clearParamsCache();
  });

  describe("parseModuleParams", () => {
    it("should parse string params correctly", () => {
      const params = '{"params": {"key": "value"}}';
      const result = parseModuleParams(params);
      expect(result).toEqual({ key: "value" });
    });

    it("should handle object params", () => {
      const params = { params: { key: "value" } };
      const result = parseModuleParams(params);
      expect(result).toEqual({ key: "value" });
    });

    it("should handle params without nested params key", () => {
      const params = { key: "value" };
      const result = parseModuleParams(params);
      expect(result).toEqual({ key: "value" });
    });

    it("should return empty object for null/undefined", () => {
      expect(parseModuleParams(null)).toEqual({});
      expect(parseModuleParams(undefined)).toEqual({});
    });
  });

  describe("getModuleParams", () => {
    it("should fetch current module params", async () => {
      const mockParams = { params: { key: "value" } };
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: ModulesParamsNamesTypes.DD,
          params: JSON.stringify(mockParams),
        }),
      });

      const result = await getModuleParams(ModulesParamsNamesTypes.DD);
      expect(result).toEqual({ params: { key: "value" } });
    });

    it("should fetch historical module params at block height", async () => {
      const mockHistoryRecord = {
        module: ModulesParamsNamesTypes.DD,
        params: JSON.stringify({ params: { key: "value" } }),
        height: 1000,
      };

      (knex as any).mockReturnValue({
        where: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        first: jest.fn().mockResolvedValue(mockHistoryRecord),
      });

      const result = await getModuleParams(ModulesParamsNamesTypes.DD, 1000);
      expect(result).toEqual({ params: { key: "value" } });
    });

    it("should return null when module not found", async () => {
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue(null),
      });

      const result = await getModuleParams(ModulesParamsNamesTypes.DD);
      expect(result).toBeNull();
    });

    it("should cache results", async () => {
      const mockParams = { params: { key: "value" } };
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: ModulesParamsNamesTypes.DD,
          params: JSON.stringify(mockParams),
        }),
      });

      const result1 = await getModuleParams(ModulesParamsNamesTypes.DD);
      const result2 = await getModuleParams(ModulesParamsNamesTypes.DD);

      expect(result1).toEqual({ params: { key: "value" } });
      expect(result2).toEqual({ params: { key: "value" } });
      expect(ModuleParams.query).toHaveBeenCalledTimes(1);
    });
  });

  describe("getModuleParamsAction", () => {
    it("should return success response with params", async () => {
      const mockParams = { params: { key: "value" } };
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: ModulesParamsNamesTypes.DD,
          params: JSON.stringify(mockParams),
        }),
      });

      const ctx = {
        meta: {},
      } as Context;

      const result = await getModuleParamsAction(ctx, ModulesParamsNamesTypes.DD, MODULE_DISPLAY_NAMES.DID_DIRECTORY);
      expect(ctx.meta.$statusCode).toBe(200);
      expect(result).toEqual({ params: { key: "value" } });
    });

    it("should return error when params not found", async () => {
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue(null),
      });

      const ctx = {
        meta: {},
      } as Context;

      const result = await getModuleParamsAction(ctx, ModulesParamsNamesTypes.DD, MODULE_DISPLAY_NAMES.DID_DIRECTORY);
      expect(ctx.meta.$statusCode).toBe(404);
      expect(result.code).toBe(404);
      expect(result.error).toBe("Module parameters not found: diddirectory");
    });

    it("should handle blockHeight from meta", async () => {
      const mockHistoryRecord = {
        module: ModulesParamsNamesTypes.DD,
        params: JSON.stringify({ params: { key: "value" } }),
        height: 1000,
      };

      (knex as any).mockReturnValue({
        where: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        first: jest.fn().mockResolvedValue(mockHistoryRecord),
      });

      const ctx = {
        meta: { blockHeight: 1000 },
      } as Context;

      const result = await getModuleParamsAction(ctx, ModulesParamsNamesTypes.DD, MODULE_DISPLAY_NAMES.DID_DIRECTORY);
      expect(ctx.meta.$statusCode).toBe(200);
      expect(result).toEqual({ params: { key: "value" } });
    });
  });

  describe("clearParamsCache", () => {
    it("should clear cache for specific module", async () => {
      const mockParams = { params: { key: "value" } };
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: ModulesParamsNamesTypes.DD,
          params: JSON.stringify(mockParams),
        }),
      });

      await getModuleParams(ModulesParamsNamesTypes.DD);
      clearParamsCache(ModulesParamsNamesTypes.DD);
      
      await getModuleParams(ModulesParamsNamesTypes.DD);
      expect(ModuleParams.query).toHaveBeenCalledTimes(2);
    });

    it("should clear all cache", async () => {
      const mockParams = { params: { key: "value" } };
      (ModuleParams.query as jest.Mock).mockReturnValue({
        findOne: jest.fn().mockResolvedValue({
          module: ModulesParamsNamesTypes.DD,
          params: JSON.stringify(mockParams),
        }),
      });

      await getModuleParams(ModulesParamsNamesTypes.DD);
      clearParamsCache();
      
      await getModuleParams(ModulesParamsNamesTypes.DD);
      expect(ModuleParams.query).toHaveBeenCalledTimes(2);
    });
  });
});

