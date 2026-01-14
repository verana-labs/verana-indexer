import { Context } from "moleculer";
import knex from "./db_connection";
import ModuleParams from "../../models/modules_params";
import ApiResponder from "./apiResponse";

const paramsCache = new Map<string, { data: { params: any }; timestamp: number }>();
const CACHE_TTL_MS = 30000;
const MAX_CACHE_SIZE = 100;

function getCacheKey(module: string, blockHeight?: number): string {
  return blockHeight ? `${module}:${blockHeight}` : module;
}

function cleanupCache(): void {
  if (paramsCache.size > MAX_CACHE_SIZE) {
    const entries = Array.from(paramsCache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toDelete = entries.slice(0, paramsCache.size - MAX_CACHE_SIZE);
    toDelete.forEach(([key]) => paramsCache.delete(key));
  }
}

export async function getModuleParams(
  module: string,
  blockHeight?: number
): Promise<{ params: any } | null> {
  try {
    const cacheKey = getCacheKey(module, blockHeight);
    const cached = paramsCache.get(cacheKey);
    const now = Date.now();

    if (cached && (now - cached.timestamp) < CACHE_TTL_MS) {
      return cached.data;
    }

    let result: { params: any } | null = null;

    if (typeof blockHeight === "number") {
      const historyRecord = await knex("module_params_history")
        .where({ module })
        .where("height", "<=", blockHeight)
        .orderBy("height", "desc")
        .orderBy("created_at", "desc")
        .first();

      if (historyRecord?.params) {
        const parsedParams = parseModuleParams(historyRecord.params);
        result = { params: parsedParams };
      }
    } else {
      const moduleRecord = await ModuleParams.query().findOne({ module });
      if (moduleRecord?.params) {
        const parsedParams = parseModuleParams(moduleRecord.params);
        result = { params: parsedParams };
      }
    }

    if (result) {
      paramsCache.set(cacheKey, { data: result, timestamp: now });
      cleanupCache();
    }

    return result;
  } catch (err) {
    console.error(`Error fetching module params for ${module}:`, err);
    return null;
  }
}

export function clearParamsCache(module?: string): void {
  if (module) {
    const keysToDelete: string[] = [];
    paramsCache.forEach((_, key) => {
      if (key.startsWith(`${module}:`) || key === module) {
        keysToDelete.push(key);
      }
    });
    keysToDelete.forEach(key => paramsCache.delete(key));
  } else {
    paramsCache.clear();
  }
}

export async function getModuleParamsAction(
  ctx: Context,
  module: string,
  moduleName: string
): Promise<any> {
  try {
    const blockHeight = (ctx.meta as any)?.blockHeight;
    const result = await getModuleParams(module, blockHeight);

    if (!result) {
      return ApiResponder.error(
        ctx,
        `Module parameters not found: ${moduleName}`,
        404
      );
    }

    return ApiResponder.success(ctx, result, 200);
  } catch (err: any) {
    console.error(`Error in getModuleParamsAction for ${moduleName}:`, err);
    return ApiResponder.error(ctx, "Internal Server Error", 500);
  }
}

export function parseModuleParams(params: any): any {
  if (!params) return {};
  
  try {
    const parsed =
      typeof params === "string" ? JSON.parse(params) : params;
    
    return parsed.params || parsed || {};
  } catch (err) {
    console.error("Error parsing module params:", err);
    return {};
  }
}

