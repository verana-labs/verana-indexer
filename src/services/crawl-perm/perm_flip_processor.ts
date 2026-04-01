import knex from "../../common/utils/db_connection";

type FlipKind = 1 | 2; // 1=ENTER_ACTIVE, 2=EXIT_ACTIVE

interface BlockContext {
  height: number;
  blockTime: Date;
}

const ENTITY_KIND = {
  GLOBAL: 0,
  TRUST_REGISTRY: 1,
  CRED_SCHEMA: 2,
  PERMISSION: 3,
} as const;

const GLOBAL_ENTITY_ID = 0;
const ROLE_TYPE_ANY = 0;

const ROLE_TYPE: Record<string, number> = {
  ECOSYSTEM: 1,
  ISSUER_GRANTOR: 2,
  ISSUER: 3,
  VERIFIER_GRANTOR: 4,
  VERIFIER: 5,
  HOLDER: 6,
};

export async function applyScheduledPermissionFlipsForBlock(
  ctx: BlockContext
): Promise<void> {
  const { height, blockTime } = ctx;

  const blockIso = blockTime.toISOString();
  const hasDue = await knex("permission_scheduled_flips")
    .where("status", 0)
    .andWhere("flip_at_time", "<=", blockIso)
    .first();
  if (!hasDue) return;

  await knex.transaction(async (trx) => {
    const flips = await trx("permission_scheduled_flips")
      .where("status", 0)
      .andWhere("flip_at_time", "<=", blockIso)
      .orderBy("flip_at_time", "asc")
      .orderBy("perm_id", "asc");

    if (!flips.length) return;

    for (const flip of flips) {
      const hasIsActiveNow = await hasPermissionsIsActiveNowColumn(trx);
      const perm = await trx("permissions")
        .select(
          "id",
          "schema_id",
          "validator_perm_id",
          "type",
          "last_valid_flip_version",
          ...(hasIsActiveNow ? (["is_active_now"] as const) : [])
        )
        .where({ id: flip.perm_id })
        .first();

      if (!perm) {
        await markFlipStale(trx, flip, height, blockTime);
        continue;
      }

      if (perm.last_valid_flip_version !== flip.version) {
        await markFlipStale(trx, flip, height, blockTime);
        continue;
      }

      const roleType = ROLE_TYPE[perm.type] ?? 0;
      if (!roleType) {
        await markFlipStale(trx, flip, height, blockTime);
        continue;
      }

      if (hasIsActiveNow) {
        const currentActive = Boolean((perm as any).is_active_now);
        const targetActive = flip.flip_kind === (1 as FlipKind);
        if (currentActive === targetActive) {
          await trx("permission_scheduled_flips")
            .where({
              perm_id: flip.perm_id,
              version: flip.version,
              flip_at_time: flip.flip_at_time,
              flip_kind: flip.flip_kind,
            })
            .update({
              status: 1,
              applied_height: height,
              applied_time: blockTime.toISOString(),
            });
          continue;
        }
      }

      const delta: number = flip.flip_kind === (1 as FlipKind) ? 1 : -1;

      const permChain: Array<{ id: number; schema_id: number }> = [];
      let currentId: number | null = perm.id;
      const visited = new Set<number>();
      const MAX_VALIDATOR_CHAIN_DEPTH = 1000;
      let traversalAborted = false;

      let depth = 0;
      while (currentId) {
        if (depth++ >= MAX_VALIDATOR_CHAIN_DEPTH) {
          traversalAborted = true;
          break;
        }
        if (visited.has(currentId)) {
          traversalAborted = true;
          break;
        }
        visited.add(currentId);
        const p = await trx("permissions")
          .select("id", "schema_id", "validator_perm_id")
          .where({ id: currentId })
          .first();
        if (!p) break;
        permChain.push({ id: p.id, schema_id: p.schema_id });
        currentId = p.validator_perm_id ?? null;
      }

      if (traversalAborted) {
        await markFlipStale(trx, flip, height, blockTime);
        continue;
      }

      const schemaId = perm.schema_id;
      const schema = await trx("credential_schemas")
        .select("id", "tr_id")
        .where({ id: schemaId })
        .first();
      const trId = schema?.tr_id ?? null;

      for (const p of permChain) {
        await bumpEntity(trx, {
          height,
          blockTime,
          entityKind: ENTITY_KIND.PERMISSION,
          entityId: p.id,
          roleType,
          delta,
        });
        await bumpEntity(trx, {
          height,
          blockTime,
          entityKind: ENTITY_KIND.PERMISSION,
          entityId: p.id,
          roleType: ROLE_TYPE_ANY,
          delta,
        });
      }

      await bumpEntity(trx, {
        height,
        blockTime,
        entityKind: ENTITY_KIND.CRED_SCHEMA,
        entityId: schemaId,
        roleType,
        delta,
      });
      await bumpEntity(trx, {
        height,
        blockTime,
        entityKind: ENTITY_KIND.CRED_SCHEMA,
        entityId: schemaId,
        roleType: ROLE_TYPE_ANY,
        delta,
      });

      if (trId != null) {
        await bumpEntity(trx, {
          height,
          blockTime,
          entityKind: ENTITY_KIND.TRUST_REGISTRY,
          entityId: trId,
          roleType,
          delta,
        });
        await bumpEntity(trx, {
          height,
          blockTime,
          entityKind: ENTITY_KIND.TRUST_REGISTRY,
          entityId: trId,
          roleType: ROLE_TYPE_ANY,
          delta,
        });
      }

      await bumpEntity(trx, {
        height,
        blockTime,
        entityKind: ENTITY_KIND.GLOBAL,
        entityId: GLOBAL_ENTITY_ID,
        roleType,
        delta,
      });
      await bumpEntity(trx, {
        height,
        blockTime,
        entityKind: ENTITY_KIND.GLOBAL,
        entityId: GLOBAL_ENTITY_ID,
        roleType: ROLE_TYPE_ANY,
        delta,
      });

      if (await hasPermissionsIsActiveNowColumn(trx)) {
        await trx("permissions")
          .where({ id: perm.id })
          .update({ is_active_now: flip.flip_kind === 1 });
      }

      await trx("permission_scheduled_flips")
        .where({
          perm_id: flip.perm_id,
          version: flip.version,
          flip_at_time: flip.flip_at_time,
          flip_kind: flip.flip_kind,
        })
        .update({
          status: 1,
          applied_height: height,
          applied_time: blockTime.toISOString(),
        });
    }
  });
}

async function markFlipStale(
  trx: any,
  flip: any,
  height: number,
  blockTime: Date
) {
  await trx("permission_scheduled_flips")
    .where({
      perm_id: flip.perm_id,
      version: flip.version,
      flip_at_time: flip.flip_at_time,
      flip_kind: flip.flip_kind,
    })
    .update({
      status: 2,
      applied_height: height,
      applied_time: blockTime.toISOString(),
    });
}

async function bumpEntity(
  trx: any,
  params: {
    height: number;
    blockTime: Date;
    entityKind: number;
    entityId: number | null;
    roleType: number;
    delta: number;
  }
) {
  const { height, blockTime, entityKind, entityId, roleType, delta } = params;

  const atHeightRow = await trx("entity_participant_changes")
    .where({
      entity_kind: entityKind,
      entity_id: entityId,
      type: roleType,
      height,
    })
    .first();

  if (atHeightRow) {
    const currentValue =
      typeof atHeightRow.value === "string" ? Number(atHeightRow.value) : Number(atHeightRow.value ?? 0);
    await trx("entity_participant_changes")
      .where({
        entity_kind: entityKind,
        entity_id: entityId,
        type: roleType,
        height,
      })
      .update({
        value: currentValue + delta,
        block_time: blockTime.toISOString(),
      });
    return;
  }

  const last = await trx("entity_participant_changes")
    .where({
      entity_kind: entityKind,
      entity_id: entityId,
      type: roleType,
    })
    .andWhere("height", "<=", height - 1)
    .orderBy("height", "desc")
    .first();

  const prev = last ? Number(last.value) : 0;
  const next = prev + delta;

  await trx("entity_participant_changes").insert({
    height,
    block_time: blockTime.toISOString(),
    entity_kind: entityKind,
    entity_id: entityId,
    type: roleType,
    value: next,
  });
}

let hasIsActiveNowColumnCache: boolean | null = null;

async function hasPermissionsIsActiveNowColumn(trx: any): Promise<boolean> {
  if (hasIsActiveNowColumnCache !== null) return hasIsActiveNowColumnCache;
  const exists = await trx.schema.hasColumn("permissions", "is_active_now");
  hasIsActiveNowColumnCache = !!exists;
  return hasIsActiveNowColumnCache;
}

