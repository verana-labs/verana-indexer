import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import knex from "../../common/utils/db_connection";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import BaseService from "../../base/base.service";
import { isValidDid } from "./api_shared";

type SnapshotRow = Record<string, unknown>;

type SnapshotResponse = {
  did: string;
  block_height: number;
  ecosystems: SnapshotRow[];
  schemas: SnapshotRow[];
  participants: SnapshotRow[];
  count: {
    ecosystems: number;
    schemas: number;
    participants: number;
  };
};

type SnapshotTables = {
  hasEcosystem: boolean;
  hasCredentialSchemas: boolean;
  hasParticipants: boolean;
  ecosystemHasHeight: boolean;
  credentialSchemasHasHeight: boolean;
  participantsHasHeight: boolean;
};

const TABLE_CHECK_TTL_MS = 60_000;
let cachedTables: { expiresAt: number; value: Promise<SnapshotTables> } | null = null;

async function getSnapshotTables(): Promise<SnapshotTables> {
  const now = Date.now();
  if (cachedTables && cachedTables.expiresAt > now) return cachedTables.value;

  const value = (async () => {
    const [hasEcosystem, hasCredentialSchemas, hasParticipants] = await Promise.all([
      knex.schema.hasTable("ecosystem"),
      knex.schema.hasTable("credential_schemas"),
      knex.schema.hasTable("participants"),
    ]);

    const [ecosystemHasHeight, credentialSchemasHasHeight, participantsHasHeight] = await Promise.all([
      hasEcosystem ? knex.schema.hasColumn("ecosystem", "height") : Promise.resolve(false),
      hasCredentialSchemas ? knex.schema.hasColumn("credential_schemas", "height") : Promise.resolve(false),
      hasParticipants ? knex.schema.hasColumn("participants", "height") : Promise.resolve(false),
    ]);

    return {
      hasEcosystem,
      hasCredentialSchemas,
      hasParticipants,
      ecosystemHasHeight,
      credentialSchemasHasHeight,
      participantsHasHeight,
    };
  })();

  cachedTables = { expiresAt: now + TABLE_CHECK_TTL_MS, value };
  return value;
}

function parseNonNegativeInteger(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isInteger(n) || n < 0) return null;
  return n;
}

async function fetchEcosystemsAtHeight(did: string, height: number, tables: SnapshotTables): Promise<SnapshotRow[]> {
  if (!tables.hasEcosystem) return [];

  const query = knex("ecosystem")
    .select("*")
    .where("did", did);

  if (tables.ecosystemHasHeight) {
    query.andWhere("height", "<=", height);
  }

  return query.orderBy("id", "asc");
}

async function fetchCredentialSchemasAtHeight(ecosystemIds: number[], _height: number, tables: SnapshotTables): Promise<SnapshotRow[]> {
  if (!tables.hasCredentialSchemas) return [];
  if (ecosystemIds.length === 0) return [];

  const query = knex("credential_schemas")
    .select("*")
    .whereIn("ecosystem_id", ecosystemIds)
    .orderBy("id", "asc");

  if (tables.credentialSchemasHasHeight) {
    query.andWhere("height", "<=", _height);
  }

  return query;
}

async function fetchParticipantsAtHeight(args: {
  did: string;
  blockHeight: number;
  schemaEcosystemIds?: number[];
  corporationIds: number[];
  tables: SnapshotTables;
}): Promise<SnapshotRow[]> {
  const { did, schemaEcosystemIds = [], corporationIds, tables, blockHeight } = args;
  if (!tables.hasParticipants) return [];

  const query = knex("participants")
    .select("*")
    .where((qb) => {
      qb.where("did", did);
      if (corporationIds.length > 0) qb.orWhere((q) => q.whereIn("corporation_id", corporationIds));
      if (tables.hasCredentialSchemas && schemaEcosystemIds.length > 0) {
        const schemaQuery = knex("credential_schemas")
          .select("id")
          .whereIn("ecosystem_id", schemaEcosystemIds);
        if (tables.credentialSchemasHasHeight) {
          schemaQuery.andWhere("height", "<=", blockHeight);
        }
        qb.orWhereIn("schema_id", schemaQuery);
      }
    })
    .orderBy("id", "asc");

  if (tables.participantsHasHeight) {
    query.andWhere("height", "<=", blockHeight);
  }

  return query;
}

export async function getDidSnapshotAtHeight(args: { did: string; blockHeight: number }): Promise<SnapshotResponse> {
  const { did, blockHeight } = args;
  const tables = await getSnapshotTables();

  const ecosystems = await fetchEcosystemsAtHeight(did, blockHeight, tables);
  const ecosystemIds = ecosystems
    .map((row) => Number(row.id))
    .filter((id) => Number.isInteger(id) && id >= 0);

  const corporationIds = Array.from(
    new Set(
      ecosystems
        .map((row) => Number((row as { corporation_id?: unknown }).corporation_id ?? 0))
        .filter((id) => Number.isInteger(id) && id > 0)
    )
  );

  const [credentialSchemas, participants] = await Promise.all([
    fetchCredentialSchemasAtHeight(ecosystemIds, blockHeight, tables),
    fetchParticipantsAtHeight({
      did,
      blockHeight,
      schemaEcosystemIds: ecosystemIds,
      corporationIds,
      tables,
    }),
  ]);

  return {
    did,
    block_height: blockHeight,
    ecosystems: ecosystems,
    schemas: credentialSchemas,
    participants,
    count: {
      ecosystems: ecosystems.length,
      schemas: credentialSchemas.length,
      participants: participants.length,
    },
  };
}

@Service({
  name: SERVICE.V1.IndexerSnapshotService.key,
  version: 1,
})
export default class IndexerSnapshotService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "getSnapshot",
    params: {
      did: { type: "string", optional: true, trim: true },
      block_height: { type: "any", optional: true },
    },
    rest: "GET /snapshot",
  })
  public async getSnapshot(ctx: Context<{ did?: string; block_height?: unknown }>) {
    try {
      const did = typeof ctx.params.did === "string" ? ctx.params.did.trim() : "";
      if (!did) return ApiResponder.error(ctx, "Missing did", 400);
      if (!isValidDid(did)) return ApiResponder.error(ctx, "Invalid did", 400);

      const blockHeight = parseNonNegativeInteger(ctx.params.block_height);
      if (blockHeight === null) {
        if (ctx.params.block_height === undefined || ctx.params.block_height === null || String(ctx.params.block_height).trim() === "") {
          return ApiResponder.error(ctx, "Missing block_height", 400);
        }
        return ApiResponder.error(ctx, "Invalid block_height", 400);
      }

      const snapshot = await getDidSnapshotAtHeight({ did, blockHeight });
      return ApiResponder.success(ctx, snapshot, 200);
    } catch (err: unknown) {
      this.logger.error("[IndexerSnapshotService] Failed to build snapshot:", err);
      if (err instanceof Errors.MoleculerError) throw err;
      throw new Errors.MoleculerError("Failed to build snapshot", 500, "SNAPSHOT_FAILED");
    }
  }
}

