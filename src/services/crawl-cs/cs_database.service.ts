import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import ModuleParams from "../../models/modules_params";

function mapToHistoryRow(row: any, overrides: Partial<any> = {}) {
  const height = Number(overrides.height) || 0;
  return {
    credential_schema_id: row.id,
    tr_id: row.tr_id,
    json_schema: row.json_schema,
    deposit: row.deposit,
    issuer_grantor_validation_validity_period: row.issuer_grantor_validation_validity_period,
    verifier_grantor_validation_validity_period: row.verifier_grantor_validation_validity_period,
    issuer_validation_validity_period: row.issuer_validation_validity_period,
    verifier_validation_validity_period: row.verifier_validation_validity_period,
    holder_validation_validity_period: row.holder_validation_validity_period,
    issuer_perm_management_mode: row.issuer_perm_management_mode,
    verifier_perm_management_mode: row.verifier_perm_management_mode,
    archived: row.archived,
    is_active: row.is_active,
    created: row.created,
    modified: row.modified,
    changes: overrides.changes ?? null,
    action: overrides.action ?? "unknown",
    created_at: knex.fn.now(),
    height: height, // Explicitly set as number
  };
}

@Service({
  name: SERVICE.V1.CredentialSchemaDatabaseService.key,
  version: 1,
})
export default class CredentialSchemaDatabaseService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "upsert" })
  async upsert(ctx: Context<{ payload: any }>) {
    try {
      const { payload } = ctx.params;

      const result = await knex.transaction(async (trx) => {
        const { height, ...schemaPayload } = payload;
        const blockHeight = Number(height) || 0;
        const [inserted] = await trx("credential_schemas")
          .insert(schemaPayload)
          .returning("*");

        const historyRow = mapToHistoryRow(inserted, {
          changes: null,
          action: "create",
          height: blockHeight,
        });
        this.logger.info(`[CS] Saving create history with height: ${blockHeight}`, historyRow);
        await trx("credential_schema_history").insert(historyRow);

        return inserted;
      });

      return ApiResponder.success(ctx, { success: true, result }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema upsert:", err);
      console.error("FATAL CS UPSERT ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({ name: "update" })
  async update(ctx: Context<{ payload: any }>) {
    try {
      const { payload } = ctx.params;

      if (!payload?.id) {
        return ApiResponder.error(ctx, "Missing required field: id", 400);
      }

      const existing = await knex("credential_schemas").where({ id: payload.id }).first();
      if (!existing) {
        return ApiResponder.error(ctx, `Credential schema with id=${payload.id} not found`, 404);
      }

      const updates: Record<string, any> = {};
      for (const [key, value] of Object.entries(payload)) {
        if (value !== undefined && key !== "id") {
          updates[key] = value;
        }
      }

      if (Object.keys(updates).length === 0) {
        return ApiResponder.error(ctx, "No valid fields to update", 400);
      }

      const { height, ...updatesWithoutHeight } = updates;
      const blockHeight = Number(height) || 0;

      const [updated] = await knex("credential_schemas")
        .where({ id: payload.id })
        .update(updatesWithoutHeight)
        .returning("*");

      const changes: Record<string, { old: any; new: any }> = {};
      for (const key of Object.keys(updatesWithoutHeight)) {
        if (existing[key] !== updated[key]) {
          changes[key] = { old: existing[key], new: updated[key] };
        }
      }

      // Only record history if there are actual changes
      if (Object.keys(changes).length > 0) {
        const historyRow = mapToHistoryRow(updated, {
          changes: changes,
          action: "update",
          height: blockHeight,
        });
        this.logger.info(`[CS] Saving update history with height: ${blockHeight}`, historyRow);
        await knex("credential_schema_history").insert(historyRow);
      }

      return ApiResponder.success(ctx, { success: true, updated }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema update:", err);
      console.error("FATAL CS UPDATE ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({ name: "archive" })
  async archive(ctx: Context<{ payload: any }>) {
    try {
      const { id, archive, modified } = ctx.params.payload;
      if (!id || archive === undefined) {
        return ApiResponder.error(ctx, "Missing required parameters: id and archive", 400);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      if (archive && schemaRecord.archived !== null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already archived`, 400);
      }
      if (!archive && schemaRecord.archived === null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already unarchived`, 400);
      }

      const updates: Record<string, any> = {
        archived: archive ? modified : null,
        is_active: archive ? true : false, // eslint-disable-line no-unneeded-ternary
        modified,
      };

      const { height } = ctx.params.payload;
      const blockHeight = Number(height) || 0;

      const [updated] = await knex("credential_schemas")
        .where({ id })
        .update(updates)
        .returning("*");
      
      const historyRow = mapToHistoryRow(updated, {
        changes: {
          archived: { old: schemaRecord.archived, new: updated.archived },
          is_active: { old: schemaRecord.is_active, new: updated.is_active },
        },
        action: archive ? "archive" : "unarchive",
        height: blockHeight,
      });
      this.logger.info(`[CS] Saving ${archive ? "archive" : "unarchive"} history with height: ${blockHeight}`, historyRow);
      await knex("credential_schema_history").insert(historyRow);

      return ApiResponder.success(ctx, { success: true, updated }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema archive:", err);
      console.error("FATAL CS ARCHIVE ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    name: "get",
    params: {
      id: { type: "number", integer: true, positive: true },
    },
  })
  async get(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const historyRecord = await knex("credential_schema_history")
          .where({ credential_schema_id: id })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
        }

        const historicalSchema = {
          id: historyRecord.credential_schema_id,
          tr_id: historyRecord.tr_id,
          json_schema: historyRecord.json_schema && typeof historyRecord.json_schema !== "string"
            ? JSON.stringify(historyRecord.json_schema)
            : historyRecord.json_schema,
          deposit: historyRecord.deposit,
          issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
          verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
          holder_validation_validity_period: historyRecord.holder_validation_validity_period,
          issuer_perm_management_mode: historyRecord.issuer_perm_management_mode,
          verifier_perm_management_mode: historyRecord.verifier_perm_management_mode,
          archived: historyRecord.archived,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        return ApiResponder.success(ctx, { schema: historicalSchema }, 200);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }
      if (schemaRecord?.json_schema && typeof schemaRecord.json_schema !== "string") {
        schemaRecord.json_schema = JSON.stringify(schemaRecord.json_schema);
      }
      delete schemaRecord?.is_active
      return ApiResponder.success(ctx, { schema: schemaRecord }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema get:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    rest: "GET list",
    params: {
      tr_id: { type: "string", optional: true },
      modified_after: { type: "string", optional: true },
      only_active: {
        type: "any",
        optional: true,
        default: false,
      },
      issuer_perm_management_mode: { type: "string", optional: true },
      verifier_perm_management_mode: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
    },
  })
  async list(ctx: Context<any>) {
    try {
      const {
        tr_id: trId,
        modified_after: modifiedAfter,
        only_active: onlyActive,
        issuer_perm_management_mode: issuerPerm,
        verifier_perm_management_mode: verifierPerm,
        response_max_size: maxSize,
      } = ctx.params;

      const blockHeight = (ctx.meta as any)?.blockHeight;
      const limit = Math.min(Math.max(maxSize || 64, 1), 1024);

      if (typeof blockHeight === "number") {
        const subquery = knex("credential_schema_history")
          .select("credential_schema_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const latestHistory = await knex
          .from(subquery)
          .select("credential_schema_id")
          .where("rn", 1);

        const schemaIdsAtHeight = latestHistory.map((r: any) => r.credential_schema_id);

        if (schemaIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { schemas: [] }, 200);
        }

        const items = await Promise.all(
          schemaIdsAtHeight.map(async (schemaId: number) => {
            const historyRecord = await knex("credential_schema_history")
              .where({ credential_schema_id: schemaId })
              .where("height", "<=", blockHeight)
              .orderBy("height", "desc")
              .orderBy("created_at", "desc")
              .first();

            if (!historyRecord) return null;

            return historyRecord;
          })
        );

        let filteredItems = items
          .filter((item): item is NonNullable<typeof items[0]> => item !== null)
          .map((historyRecord) => ({
            id: historyRecord.credential_schema_id,
            tr_id: historyRecord.tr_id,
            json_schema:
              historyRecord.json_schema && typeof historyRecord.json_schema !== "string"
                ? JSON.stringify(historyRecord.json_schema)
                : historyRecord.json_schema,
            deposit: historyRecord.deposit,
            issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
            verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
            issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
            verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
            holder_validation_validity_period: historyRecord.holder_validation_validity_period,
            issuer_perm_management_mode: historyRecord.issuer_perm_management_mode,
            verifier_perm_management_mode: historyRecord.verifier_perm_management_mode,
            archived: historyRecord.archived,
            created: historyRecord.created,
            modified: historyRecord.modified,
            is_active: historyRecord.is_active,
          }));

        if (trId) filteredItems = filteredItems.filter(item => String(item.tr_id) === String(trId));
        if (modifiedAfter) {
          const ts = new Date(modifiedAfter);
          if (Number.isNaN(ts.getTime())) {
            return ApiResponder.error(ctx, "Invalid modified_after timestamp", 400);
          }
          filteredItems = filteredItems.filter(item => new Date(item.modified) > ts);
        }

        let onlyActiveBool: boolean | undefined;
        if (typeof onlyActive === "string") {
          onlyActiveBool = onlyActive.toLowerCase() === "true";
        } else if (typeof onlyActive === "boolean") {
          onlyActiveBool = onlyActive;
        }

        if (onlyActiveBool === true) {
          filteredItems = filteredItems.filter(item => item.is_active === true);
        } else if (onlyActiveBool === false) {
          filteredItems = filteredItems.filter(item => item.is_active === false);
        }

        const finalItems = filteredItems.map(({ is_active, ...rest }) => rest);
        filteredItems = finalItems as typeof filteredItems;

        if (issuerPerm !== undefined) {
          filteredItems = filteredItems.filter(item => item.issuer_perm_management_mode === issuerPerm);
        }

        if (verifierPerm !== undefined) {
          filteredItems = filteredItems.filter(item => item.verifier_perm_management_mode === verifierPerm);
        }

        filteredItems.sort((a, b) => new Date(a.modified).getTime() - new Date(b.modified).getTime());
        filteredItems = filteredItems.slice(0, limit);

        return ApiResponder.success(ctx, { schemas: filteredItems }, 200);
      }

      const query = knex("credential_schemas");
      if (trId) query.where("tr_id", trId);

      if (modifiedAfter) {
        const ts = new Date(modifiedAfter);
        if (Number.isNaN(ts.getTime())) {
          return ApiResponder.error(ctx, "Invalid modified_after timestamp", 400);
        }
        query.where("modified", ">", ts.toISOString());
      }
      let onlyActiveBool: boolean | undefined;
      if (typeof onlyActive === "string") {
        onlyActiveBool = onlyActive.toLowerCase() === "true";
      } else if (typeof onlyActive === "boolean") {
        onlyActiveBool = onlyActive;
      }

      if (onlyActiveBool === true) {
        query.where("is_active", true);
      } else if (onlyActiveBool === false) {
        query.where("is_active", false);
      }

      if (issuerPerm !== undefined) {
        query.where("issuer_perm_management_mode", issuerPerm);
      }

      if (verifierPerm !== undefined) {
        query.where("verifier_perm_management_mode", verifierPerm);
      }
      const items = await query.orderBy("modified", "asc").limit(limit);

      const cleanItems = items?.map(({ is_active, ...rest }) => ({
        ...rest,
        json_schema:
          rest.json_schema && typeof rest.json_schema !== "string"
            ? JSON.stringify(rest.json_schema)
            : rest.json_schema,
      }));

      return ApiResponder.success(ctx, { schemas: cleanItems }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema list:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    rest: "GET JsonSchema",
    params: {
      id: { type: "number", integer: true, positive: true },
    },
  })
  async JsonSchema(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const historyRecord = await knex("credential_schema_history")
          .select("json_schema")
          .where({ credential_schema_id: id })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
        }

        return ApiResponder.success(ctx, { schema: JSON.stringify(historyRecord.json_schema) }, 200);
      }

      const schemaRecord = await knex("credential_schemas")
        .select("json_schema")
        .where({ id })
        .first();

      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      return ApiResponder.success(ctx, { schema: JSON.stringify(schemaRecord.json_schema) }, 200);
    } catch (err: any) {
      this.logger.error("Error in renderJsonSchema:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action()
  public async getParams(ctx: Context) {
    try {
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const historyRecord = await knex("module_params_history")
          .where({ module: ModulesParamsNamesTypes?.CS })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord || !historyRecord.params) {
          return ApiResponder.error(ctx, "Module parameters not found: credentialschema", 404);
        }

        const parsedParams =
          typeof historyRecord.params === "string"
            ? JSON.parse(historyRecord.params)
            : historyRecord.params;

        return ApiResponder.success(ctx, { params: parsedParams.params || parsedParams }, 200);
      }

      const module = await ModuleParams.query().findOne({ module: ModulesParamsNamesTypes?.CS });

      if (!module || !module.params) {
        return ApiResponder.error(ctx, "Module parameters not found: credentialschema", 404);
      }

      const parsedParams =
        typeof module.params === "string"
          ? JSON.parse(module.params)
          : module.params;

      return ApiResponder.success(ctx, { params: parsedParams.params }, 200);
    } catch (err: any) {
      this.logger.error("Error fetching credentialschema params", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    name: "getHistory",
    params: {
      id: { type: "number", integer: true, positive: true },
    },
  })
  async getHistory(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params;

      const schemaExists = await knex("credential_schemas").where({ id }).first();
      if (!schemaExists) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      const historyRecords = await knex("credential_schema_history")
        .where({ credential_schema_id: id })
        .orderBy("created_at", "asc");

      const cleanHistory = historyRecords.map(record => ({
        ...record,
        json_schema:
          record.json_schema && typeof record.json_schema !== "string"
            ? JSON.stringify(record.json_schema)
            : record.json_schema,
      }));
      const csResult = { id, history: cleanHistory }
      return ApiResponder.success(ctx, { schema: csResult }, 200);
    } catch (err: any) {
      this.logger.error("Error fetching CredentialSchema history:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

}
