import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import ApiResponder from "../../common/utils/apiResponse";

@Service({
  name: SERVICE.V1.AccountReputationService.key,
  version: 1,
})
export default class AccountReputationService extends BullableService {
  constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "getAccountReputation",
    params: {
      account: { type: "string" },
      tr_id: { type: "number", optional: true },
      schema_id: { type: "number", optional: true },
      include_slash_details: {
        type: "boolean",
        optional: true,
        default: false,
        convert: true
      },
    },
  })
  public async getAccountReputation(ctx: Context<{
    account: string;
    tr_id?: number;
    schema_id?: number;
    include_slash_details?: boolean | string;
  }>) {
    const { account, tr_id: trId, schema_id: schemaId, include_slash_details: includeSlashDetails } = ctx.params;
    const blockHeight = (ctx.meta as any)?.blockHeight;

    if (!this.isValidAccount(account)) {
      return ApiResponder.error(ctx, "Invalid account format", 400);
    }

    const accountRow = await knex("account").where({ address: account }).first();
    if (!accountRow) return ApiResponder.error(ctx, "Account not found", 404);

    const balance = (() => {
      try {
        const balances = Array.isArray(accountRow.spendable_balances) ? accountRow.spendable_balances : [];
        const uvna = balances?.find((b: { denom?: string; amount?: string }) => b?.denom === "uvna");
        return uvna?.amount ?? "0";
      } catch {
        return "0";
      }
    })();

    let td: any;
    if (typeof blockHeight === "number") {
      const tdHistory = await knex("trust_deposit_history")
        .where({ account })
        .where("height", "<=", blockHeight)
        .orderBy("height", "desc")
        .orderBy("created_at", "desc")
        .first();
      td = tdHistory ? {
        amount: tdHistory.amount != null ? Number(tdHistory.amount) : 0,
        slashed_deposit: tdHistory.slashed_deposit != null ? Number(tdHistory.slashed_deposit) : 0,
        repaid_deposit: tdHistory.repaid_deposit != null ? Number(tdHistory.repaid_deposit) : 0,
        slash_count: tdHistory.slash_count || 0,
        last_slashed: tdHistory.last_slashed,
        last_repaid: tdHistory.last_repaid,
        last_repaid_by: tdHistory.last_repaid_by,
      } : null;
    } else {
      td = await knex("trust_deposits").where({ account }).first();
    }

    const deposit = td?.amount || "0";
    const slashed = td?.slashed_deposit || "0";
    const repaid = td?.repaid_deposit || "0";
    const slashCount = td?.slash_count || 0;

    const slashDetails = includeSlashDetails
      ? (typeof blockHeight === "number"
          ? await knex("trust_deposit_history")
              .where({ account })
              .where("height", "<=", blockHeight)
              .whereNotNull("last_slashed")
              .select(
                "slashed_deposit as slashed_amount",
                "last_slashed as slashed_ts"
              )
              .orderBy("height", "desc")
              .then(rows => rows.map(row => ({
                ...row,
                slashed_by: account
              })))
          : await knex("trust_deposits")
              .where({ account })
              .select(
                "slashed_deposit as slashed_amount",
                "last_slashed as slashed_ts"
              )
              .whereNotNull("last_slashed")
              .then(rows => rows.map(row => ({
                ...row,
                slashed_by: account
              }))))
      : [];

    const repayDetails = includeSlashDetails
      ? (typeof blockHeight === "number"
          ? await knex("trust_deposit_history")
              .where({ account })
              .where("height", "<=", blockHeight)
              .whereNotNull("last_repaid")
              .select(
                "repaid_deposit as repaid_amount",
                "last_repaid as repaid_ts",
                "last_repaid_by as repaid_by"
              )
              .orderBy("height", "desc")
          : await knex("trust_deposits")
              .where({ account })
              .select(
                "repaid_deposit as repaid_amount",
                "last_repaid as repaid_ts",
                "last_repaid_by as repaid_by"
              )
              .whereNotNull("last_repaid"))
      : [];

    let permissionRows: any[];
    if (typeof blockHeight === "number") {
      const permHistory = await knex("permission_history")
        .where("grantee", account)
        .where("height", "<=", blockHeight)
        .orderBy("height", "desc")
        .orderBy("created_at", "desc");

      const permMap = new Map<string, any>();
      for (const perm of permHistory) {
        if (!permMap.has(String(perm.permission_id))) {
          permMap.set(String(perm.permission_id), perm);
        }
      }

      permissionRows = await Promise.all(
        Array.from(permMap.values()).map(async (perm: any) => {
          const schemaHistory = await knex("credential_schema_history")
            .where({ credential_schema_id: perm.schema_id })
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();
          
          if (!schemaHistory) return null;

          const trHistory = await knex("trust_registry_history")
            .where({ tr_id: schemaHistory.tr_id })
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();

          if (trId && String(schemaHistory.tr_id) !== String(trId)) return null;
          if (schemaId && schemaHistory.credential_schema_id !== schemaId) return null;

          return {
            tr_id: schemaHistory.tr_id,
            tr_did: trHistory?.did || null,
            schema_id: schemaHistory.credential_schema_id,
            schema_deposit: schemaHistory.deposit,
            type: perm.type,
            revoked: perm.revoked,
            effective_until: perm.effective_until,
            slashed: perm.slashed,
            repaid: perm.repaid,
            deposit: perm.deposit,
            slashed_deposit: perm.slashed_deposit,
            repaid_deposit: perm.repaid_deposit,
            perm_id: perm.permission_id,
          };
        })
      );
      permissionRows = permissionRows.filter((r): r is NonNullable<typeof permissionRows[0]> => r !== null);
    } else {
      let permissionsQuery = knex("permissions as p")
        .joinRaw("LEFT JOIN credential_schemas cs on p.schema_id::text = cs.id::text")
        .joinRaw("LEFT JOIN trust_registry tr on tr.id::text = cs.tr_id::text")
        .joinRaw("LEFT JOIN permission_sessions ps on p.id::text = ps.agent_perm_id::text")
        .where("p.grantee", account);

      if (trId) permissionsQuery = permissionsQuery.andWhere("cs.tr_id", String(trId));
      if (schemaId) permissionsQuery = permissionsQuery.andWhere("cs.id", schemaId);

      permissionRows = await permissionsQuery.select(
        "tr.id as tr_id",
        "tr.did as tr_did",
        "cs.id as schema_id",
        "cs.deposit as schema_deposit",
        "p.type",
        "p.revoked",
        "p.effective_until",
        "p.slashed",
        "p.repaid",
        "p.deposit",
        "p.slashed_deposit",
        "p.repaid_deposit",
        "p.id as perm_id"
      );
    }


    type SchemaStats = {
      schema_id?: number;
      deposit: number;
      slashed: number;
      repaid: number;
      slash_count: number;
      issued: number;
      verified: number;
      run_as_validator_vps: number;
      run_as_applicant_vps: number;
      issuer_perm_count: number;
      verifier_perm_count: number;
      issuer_grantor_perm_count: number;
      verifier_grantor_perm_count: number;
      ecosystem_perm_count: number;
      active_issuer_perm_count: number;
      active_verifier_perm_count: number;
      active_issuer_grantor_perm_count: number;
      active_verifier_grantor_perm_count: number;
      active_ecosystem_perm_count: number;
      slashs?: unknown[];
      repayments?: unknown[];
    };

    type Registry = {
      tr_id?: number;
      tr_did?: string;
      credential_schemas: Record<string, SchemaStats>;
    };

    const registriesMap: Record<string, Registry> = {};
    for (const row of permissionRows) {
      if (!registriesMap[row.tr_id]) {
        registriesMap[row.tr_id] = {
          tr_id: row.tr_id,
          tr_did: row.tr_did,
          credential_schemas: {},
        };
      }

      if (!registriesMap[row.tr_id].credential_schemas[row.schema_id]) {
        const newSchema: SchemaStats = {
          schema_id: row.schema_id,
          deposit: 0,
          slashed: 0,
          repaid: 0,
          slash_count: 0,
          issued: 0,
          verified: 0,
          run_as_validator_vps: 0,
          run_as_applicant_vps: 0,
          issuer_perm_count: 0,
          verifier_perm_count: 0,
          issuer_grantor_perm_count: 0,
          verifier_grantor_perm_count: 0,
          ecosystem_perm_count: 0,
          active_issuer_perm_count: 0,
          active_verifier_perm_count: 0,
          active_issuer_grantor_perm_count: 0,
          active_verifier_grantor_perm_count: 0,
          active_ecosystem_perm_count: 0,
        };

        if (includeSlashDetails) {
          newSchema.slashs = [];
          newSchema.repayments = [];
        }

        registriesMap[row.tr_id].credential_schemas[row.schema_id] = newSchema;
      }

      const schema = registriesMap[row.tr_id].credential_schemas[row.schema_id];

      schema.deposit += Number(row.deposit || 0);
      schema.slashed += Number(row.slashed_deposit || 0);
      schema.repaid += Number(row.repaid_deposit || 0);
      schema.slash_count += row.slashed ? 1 : 0;

      switch (row.type) {
        case "ISSUER":
          schema.issuer_perm_count++;
          if (!row.revoked && (!row.effective_until || new Date(row.effective_until) > new Date()) && (!row.slashed || row.repaid)) {
            schema.active_issuer_perm_count++;
          }
          break;
        case "VERIFIER":
          schema.verifier_perm_count++;
          if (!row.revoked && (!row.effective_until || new Date(row.effective_until) > new Date()) && (!row.slashed || row.repaid)) {
            schema.active_verifier_perm_count++;
          }
          break;
        case "ISSUER_GRANTOR":
          schema.issuer_grantor_perm_count++;
          if (!row.revoked && (!row.effective_until || new Date(row.effective_until) > new Date()) && (!row.slashed || row.repaid)) {
            schema.active_issuer_grantor_perm_count++;
          }
          break;
        case "VERIFIER_GRANTOR":
          schema.verifier_grantor_perm_count++;
          if (!row.revoked && (!row.effective_until || new Date(row.effective_until) > new Date()) && (!row.slashed || row.repaid)) {
            schema.active_verifier_grantor_perm_count++;
          }
          break;
        case "ECOSYSTEM":
          schema.ecosystem_perm_count++;
          if (!row.revoked && (!row.effective_until || new Date(row.effective_until) > new Date()) && (!row.slashed || row.repaid)) {
            schema.active_ecosystem_perm_count++;
          }
          break;
      }
      schema.issued = 0;
      schema.verified = 0;
      schema.run_as_validator_vps = 0;
      schema.run_as_applicant_vps = 0;

      if (includeSlashDetails) {
        if (row.slashed) {
          if (schema.slashs) {
            schema.slashs.push({
              perm_id: row.perm_id,
              schema_id: row.schema_id,
              tr_id: row.tr_id,
              slashed_ts: row.slashed,
              slashed_by: account,
            });
          }
        }
        if (row.repaid) {
          if (schema.repayments) {
            schema.repayments.push({
              perm_id: row.perm_id,
              schema_id: row.schema_id,
              tr_id: row.tr_id,
              repaid_ts: row.repaid,
              repaid_by: account,
            });
          }
        }
      }
    }

    const trustRegistries = Object.values(registriesMap).map((tr: Registry) => ({
      tr_id: tr.tr_id,
      tr_did: tr.tr_did,
      credential_schemas: Object.values(tr.credential_schemas).map(schema => {
        const baseSchema = {
          schema_id: schema.schema_id,
          deposit: schema.deposit,
          slashed: schema.slashed,
          repaid: schema.repaid,
          slash_count: schema.slash_count,
          issued: schema.issued,
          verified: schema.verified,
          run_as_validator_vps: schema.run_as_validator_vps,
          run_as_applicant_vps: schema.run_as_applicant_vps,
          issuer_perm_count: schema.issuer_perm_count,
          verifier_perm_count: schema.verifier_perm_count,
          issuer_grantor_perm_count: schema.issuer_grantor_perm_count,
          verifier_grantor_perm_count: schema.verifier_grantor_perm_count,
          ecosystem_perm_count: schema.ecosystem_perm_count,
          active_issuer_perm_count: schema.active_issuer_perm_count,
          active_verifier_perm_count: schema.active_verifier_perm_count,
          active_issuer_grantor_perm_count: schema.active_issuer_grantor_perm_count,
          active_verifier_grantor_perm_count: schema.active_verifier_grantor_perm_count,
          active_ecosystem_perm_count: schema.active_ecosystem_perm_count
        };

        if (includeSlashDetails) {
          return {
            ...baseSchema,
            slashs: schema.slashs,
            repayments: schema.repayments
          };
        }

        return baseSchema;
      }),
    }));

    const responseData = {
      account,
      balance: String(balance),
      deposit: Number(deposit),
      slashed: Number(slashed),
      repaid: Number(repaid),
      slash_count: slashCount,
      first_interaction_ts: accountRow.first_interaction_ts || null,
      trust_registry_count: trustRegistries.length,
      credential_schema_count: trustRegistries.reduce(
        (sum: number, tr: { credential_schemas?: unknown[] }) =>
          sum + (tr.credential_schemas ? tr.credential_schemas.length : 0),
        0
      ),
      ...(includeSlashDetails ? {
        slashs: slashDetails,
        repayments: repayDetails
      } : {}),
      trust_registries: trustRegistries,
    };

    return ApiResponder.success(ctx, responseData);
  }

  private isValidAccount(account: string): boolean {
    return /^verana1[0-9a-z]{10,}$/.test(account);
  }
}
