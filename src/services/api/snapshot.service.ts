import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import knex from "../../common/utils/db_connection";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import BaseService from "../../base/base.service";
import { isValidDid } from "./api_shared";

const BLOCK_CHECKPOINT_JOB = BULL_JOB_NAME.HANDLE_TRANSACTION;

async function fetchLatestIndexedHeight(): Promise<number> {
  const checkpoint = await knex("block_checkpoint")
    .select("height")
    .where("job_name", BLOCK_CHECKPOINT_JOB)
    .first();
  const height = Number(checkpoint?.height ?? 0);
  return Number.isInteger(height) && height >= 0 ? height : 0;
}

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

function uniquePositiveIds(values: unknown[]): number[] {
  return Array.from(
    new Set(
      values
        .map((value) => Number(value ?? 0))
        .filter((id) => Number.isInteger(id) && id > 0)
    )
  );
}

async function fetchEcosystemsByDidOrIds(
  did: string,
  ids: number[],
  height: number,
  tables: SnapshotTables
): Promise<SnapshotRow[]> {
  if (!tables.hasEcosystem) return [];

  const query = knex("ecosystem")
    .select("*")
    .where((qb) => {
      qb.where("did", did);
      if (ids.length > 0) qb.orWhereIn("id", ids);
    });

  if (tables.ecosystemHasHeight) {
    query.andWhere("height", "<=", height);
  }

  return query.orderBy("id", "asc");
}

async function fetchParticipantsByDid(did: string, height: number, tables: SnapshotTables): Promise<SnapshotRow[]> {
  if (!tables.hasParticipants) return [];

  const query = knex("participants").select("*").where("did", did);
  if (tables.participantsHasHeight) {
    query.andWhere("height", "<=", height);
  }

  return query.orderBy("id", "asc");
}

async function fetchCredentialSchemas(args: {
  ecosystemIds: number[];
  schemaIds: number[];
  height: number;
  tables: SnapshotTables;
}): Promise<SnapshotRow[]> {
  const { ecosystemIds, schemaIds, height, tables } = args;
  if (!tables.hasCredentialSchemas) return [];
  if (ecosystemIds.length === 0 && schemaIds.length === 0) return [];

  const query = knex("credential_schemas")
    .select("*")
    .where((qb) => {
      if (ecosystemIds.length > 0) qb.orWhereIn("ecosystem_id", ecosystemIds);
      if (schemaIds.length > 0) qb.orWhereIn("id", schemaIds);
    })
    .orderBy("id", "asc");

  if (tables.credentialSchemasHasHeight) {
    query.andWhere("height", "<=", height);
  }

  return query;
}

async function fetchParticipants(args: {
  did: string;
  blockHeight: number;
  schemaIds: number[];
  corporationIds: number[];
  tables: SnapshotTables;
}): Promise<SnapshotRow[]> {
  const { did, schemaIds, corporationIds, tables, blockHeight } = args;
  if (!tables.hasParticipants) return [];

  const query = knex("participants")
    .select("*")
    .where((qb) => {
      qb.where("did", did);
      if (corporationIds.length > 0) qb.orWhereIn("corporation_id", corporationIds);
      if (schemaIds.length > 0) qb.orWhereIn("schema_id", schemaIds);
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

  // Seed the graph from the two places a DID can appear directly: an ecosystem's
  // controller DID and a participant's DID.
  const [ecosystemsByDid, participantsByDid] = await Promise.all([
    fetchEcosystemsByDidOrIds(did, [], blockHeight, tables),
    fetchParticipantsByDid(did, blockHeight, tables),
  ]);

  // Schemas reachable either forward (under a DID-matched ecosystem) or in reverse
  // (referenced by a DID-matched participant's schema_id).
  const schemas = await fetchCredentialSchemas({
    ecosystemIds: uniquePositiveIds(ecosystemsByDid.map((row) => row.id)),
    schemaIds: uniquePositiveIds(participantsByDid.map((row) => row.schema_id)),
    height: blockHeight,
    tables,
  });

  // Ecosystems owning those schemas (reverse link), merged with the DID-matched ones.
  const ecosystems = await fetchEcosystemsByDidOrIds(
    did,
    uniquePositiveIds(schemas.map((row) => row.ecosystem_id)),
    blockHeight,
    tables
  );

  const corporationIds = uniquePositiveIds([
    ...ecosystems.map((row) => (row as { corporation_id?: unknown }).corporation_id),
    ...participantsByDid.map((row) => (row as { corporation_id?: unknown }).corporation_id),
  ]);

  const participants = await fetchParticipants({
    did,
    blockHeight,
    schemaIds: uniquePositiveIds(schemas.map((row) => row.id)),
    corporationIds,
    tables,
  });

  return {
    did,
    block_height: blockHeight,
    ecosystems,
    schemas,
    participants,
    count: {
      ecosystems: ecosystems.length,
      schemas: schemas.length,
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
    },
    rest: "GET /snapshot",
  })
  public async getSnapshot(ctx: Context<{ did?: string }>) {
    try {
      const did = typeof ctx.params.did === "string" ? ctx.params.did.trim() : "";
      if (!did) return ApiResponder.error(ctx, "Missing did", 400);
      if (!isValidDid(did)) return ApiResponder.error(ctx, "Invalid did", 400);

      const headerHeight = (ctx.meta as { blockHeight?: number } | undefined)?.blockHeight;
      const blockHeight =
        typeof headerHeight === "number" && Number.isInteger(headerHeight) && headerHeight >= 0
          ? headerHeight
          : await fetchLatestIndexedHeight();

      const snapshot = await getDidSnapshotAtHeight({ did, blockHeight });
      return ApiResponder.success(ctx, snapshot, 200);
    } catch (err: unknown) {
      this.logger.error("[IndexerSnapshotService] Failed to build snapshot:", err);
      if (err instanceof Errors.MoleculerError) throw err;
      throw new Errors.MoleculerError("Failed to build snapshot", 500, "SNAPSHOT_FAILED");
    }
  }
}

