import { Context } from "moleculer";
import knex from "./db_connection";
import ApiResponder from "./apiResponse";
import { getBlockHeight, hasBlockHeight } from "./blockHeight";
import { ModulesParamsNamesTypes } from "../constant";
import ModuleParams from "../../models/modules_params";

export async function getModuleParams(
  ctx: Context,
  moduleName: ModulesParamsNamesTypes,
  moduleDisplayName: string,
  logger: any
) {
  try {
    const blockHeight = getBlockHeight(ctx);

    if (hasBlockHeight(ctx) && blockHeight !== undefined) {
      const historyRecord = await knex("module_params_history")
        .where({ module: moduleName })
        .where("height", "<=", blockHeight)
        .orderBy("height", "desc")
        .orderBy("created_at", "desc")
        .first();

      if (!historyRecord || !historyRecord.params) {
        return ApiResponder.error(
          ctx,
          `Module parameters not found: ${moduleDisplayName}`,
          404
        );
      }

      const parsedParams =
        typeof historyRecord.params === "string"
          ? JSON.parse(historyRecord.params)
          : historyRecord.params;

      return ApiResponder.success(
        ctx,
        { params: parsedParams.params || parsedParams },
        200
      );
    }

    const module = await ModuleParams.query().findOne({ module: moduleName });

    if (!module || !module.params) {
      return ApiResponder.error(
        ctx,
        `Module parameters not found: ${moduleDisplayName}`,
        404
      );
    }

    const parsedParams =
      typeof module.params === "string"
        ? JSON.parse(module.params)
        : module.params;

    return ApiResponder.success(ctx, { params: parsedParams.params || parsedParams }, 200);
  } catch (err: unknown) {
    logger.error(`Error fetching ${moduleDisplayName} params`, err);
    return ApiResponder.error(ctx, "Internal Server Error", 500);
  }
}

