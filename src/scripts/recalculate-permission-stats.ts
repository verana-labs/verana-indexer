import * as dotenv from "dotenv";
import knex, { Knex } from "knex";
import { loadEnvFiles } from "../common/utils/loadEnv";
import { getConfigForEnv } from "../knexfile";
import { calculatePermState } from "../services/crawl-perm/perm_state_utils";

loadEnvFiles();
dotenv.config();

if (global.gc) {
  console.log("✅ Garbage collection is available");
} else {
  console.warn("⚠️  Garbage collection not available. Run with --expose-gc flag for better memory management.");
}

async function getBottomUpOrder(db: Knex, schemaId: string): Promise<string[]> {
  const allPerms = await db("permissions")
    .where("schema_id", schemaId)
    .select("id", "validator_perm_id")
    .timeout(60000);

  const childrenMap = new Map<string, string[]>();
  const permSet = new Set<string>();

  for (const perm of allPerms) {
    const id = String(perm.id);
    permSet.add(id);
    const parentId = perm.validator_perm_id ? String(perm.validator_perm_id) : null;

    if (parentId) {
      if (!childrenMap.has(parentId)) {
        childrenMap.set(parentId, []);
      }
      childrenMap.get(parentId)!.push(id);
    }
  }

  const roots: string[] = [];
  for (const id of permSet) {
    const perm = allPerms.find(p => String(p.id) === id);
    if (!perm?.validator_perm_id) {
      roots.push(id);
    }
  }

  const result: string[] = [];
  const visited = new Set<string>();

  function traverse(permId: string) {
    if (visited.has(permId)) return;
    visited.add(permId);

    const children = childrenMap.get(permId) || [];
    for (const childId of children) {
      traverse(childId);
    }

    result.push(permId);
  }

  for (const root of roots) {
    traverse(root);
  }

  return result;
}

async function calculateWeightFromDB(
  db: Knex,
  permId: string,
  schemaId: string
): Promise<string> {
  const perm = await db("permissions")
    .where({ id: permId, schema_id: schemaId })
    .select("deposit", "weight")
    .timeout(30000)
    .first();

  if (!perm) return "0";

  const ownDeposit = BigInt(perm.deposit || "0");

  const children = await db("permissions")
    .where("validator_perm_id", permId)
    .where("schema_id", schemaId)
    .select("weight")
    .timeout(30000);

  let childWeightSum = BigInt(0);
  for (const child of children) {
    const childWeight = child.weight ? BigInt(child.weight) : BigInt(0);
    childWeightSum += childWeight;
  }

  return (ownDeposit + childWeightSum).toString();
}

async function calculateParticipants(
  db: Knex,
  permId: string,
  schemaId: string,
  now: Date = new Date()
): Promise<number> {
  const perm = await db("permissions")
    .where({ id: permId, schema_id: schemaId })
    .timeout(30000)
    .first();

  if (!perm) return 0;

  const permState = calculatePermState(
    {
      repaid: perm.repaid,
      slashed: perm.slashed,
      revoked: perm.revoked,
      effective_from: perm.effective_from,
      effective_until: perm.effective_until,
      type: perm.type,
      vp_state: perm.vp_state,
      vp_exp: perm.vp_exp,
      validator_perm_id: perm.validator_perm_id,
    },
    now
  );

  let count = permState === "ACTIVE" ? 1 : 0;

  const children = await db("permissions")
    .where("validator_perm_id", permId)
    .where("schema_id", schemaId)
    .select("id", "repaid", "slashed", "revoked", "effective_from", "effective_until", "type", "vp_state", "vp_exp", "validator_perm_id")
    .timeout(30000);

  for (const child of children) {
    const childState = calculatePermState(
      {
        repaid: child.repaid,
        slashed: child.slashed,
        revoked: child.revoked,
        effective_from: child.effective_from,
        effective_until: child.effective_until,
        type: child.type,
        vp_state: child.vp_state,
        vp_exp: child.vp_exp,
        validator_perm_id: child.validator_perm_id,
      },
      now
    );

    if (childState === "ACTIVE") {
      count++;
    }

    const childCount = await calculateParticipants(db, String(child.id), schemaId, now);
    count += childCount;
  }

  return count;
}

async function calculateIssuedVerified(
  db: Knex,
  permId: string,
  schemaId: string,
  sessionsCache?: Array<{ authz: any }>
): Promise<{ issued: string; verified: string }> {
  const permissionIds = new Set<string>();
  let currentPermId: string | null = permId;

  while (currentPermId) {
    permissionIds.add(currentPermId);
    const perm: { validator_perm_id: string | null } | undefined = await db("permissions")
      .where("id", currentPermId)
      .where("schema_id", schemaId)
      .select("validator_perm_id")
      .timeout(30000)
      .first();

    currentPermId = perm?.validator_perm_id || null;
  }

  if (permissionIds.size === 0) {
    return { issued: "0", verified: "0" };
  }

  let issuedCount = BigInt(0);
  let verifiedCount = BigInt(0);

  const sessions = sessionsCache || await db("permission_sessions")
    .select("authz")
    .timeout(60000);

  for (const session of sessions) {
    const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
    if (Array.isArray(authz)) {
      for (const entry of authz) {
        if (entry.issuer_perm_id && permissionIds.has(String(entry.issuer_perm_id))) {
          issuedCount += BigInt(1);
        }
        if (entry.verifier_perm_id && permissionIds.has(String(entry.verifier_perm_id))) {
          verifiedCount += BigInt(1);
        }
      }
    }
  }

  return {
    issued: issuedCount.toString(),
    verified: verifiedCount.toString(),
  };
}

async function calculateSlashStatistics(
  db: Knex,
  permId: string,
  schemaId: string
): Promise<{
  ecosystem_slash_events: number;
  ecosystem_slashed_amount: string;
  ecosystem_slashed_amount_repaid: string;
  network_slash_events: number;
  network_slashed_amount: string;
  network_slashed_amount_repaid: string;
}> {
  const schema = await db("credential_schemas")
    .where("id", String(schemaId))
    .timeout(30000)
    .first();
  
  let trController: string | null = null;
  if (schema?.tr_id) {
    const tr = await db("trust_registry")
      .where("id", schema.tr_id)
      .timeout(30000)
      .first();
    trController = tr?.controller || null;
  }

  const permissionIds = new Set<string>();
  let currentPermId: string | null = permId;

  while (currentPermId) {
    permissionIds.add(currentPermId);
    const perm: { validator_perm_id: string | null; type: string } | undefined = await db("permissions")
      .where("id", currentPermId)
      .where("schema_id", String(schemaId))
      .select("validator_perm_id", "type")
      .timeout(30000)
      .first();

    currentPermId = perm?.validator_perm_id || null;
  }

  const slashEvents = await db("permission_history")
    .whereIn("permission_id", Array.from(permissionIds))
    .where("schema_id", String(schemaId))
    .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
    .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
    .orderBy("permission_id", "asc")
    .orderBy("height", "asc")
    .orderBy("created_at", "asc")
    .timeout(60000);

  let ecosystemSlashEvents = 0;
  let ecosystemSlashedAmount = BigInt(0);
  let ecosystemSlashedAmountRepaid = BigInt(0);
  let networkSlashEvents = 0;
  let networkSlashedAmount = BigInt(0);
  let networkSlashedAmountRepaid = BigInt(0);

  const prevSlashedDeposits = new Map<string, string>();
  const prevRepaidDeposits = new Map<string, string>();

  for (const event of slashEvents) {
    const permIdStr = String(event.permission_id);
    const prevSlashed = prevSlashedDeposits.get(permIdStr) || "0";
    const currentSlashed = event.slashed_deposit || "0";
    const incrementalSlashed = BigInt(currentSlashed) - BigInt(prevSlashed);
    
    if (incrementalSlashed <= 0) {
      prevSlashedDeposits.set(permIdStr, currentSlashed);
      const currentRepaid = event.repaid_deposit || "0";
      prevRepaidDeposits.set(permIdStr, currentRepaid);
      continue;
    }

    prevSlashedDeposits.set(permIdStr, currentSlashed);

    const isEcosystemPermission = event.type === "ECOSYSTEM";
    const isSlashedByEcosystemGov = trController && event.slashed_by === trController;

    if (isEcosystemPermission) {
      networkSlashEvents++;
      networkSlashedAmount += incrementalSlashed;
      
      const repaid = event.repaid_deposit || "0";
      const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
      const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
      if (incrementalRepaid > 0) {
        networkSlashedAmountRepaid += incrementalRepaid;
      }
      prevRepaidDeposits.set(permIdStr, repaid);
    } else if (isSlashedByEcosystemGov) {
      ecosystemSlashEvents++;
      ecosystemSlashedAmount += incrementalSlashed;
      
      const repaid = event.repaid_deposit || "0";
      const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
      const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
      if (incrementalRepaid > 0) {
        ecosystemSlashedAmountRepaid += incrementalRepaid;
      }
      prevRepaidDeposits.set(permIdStr, repaid);
    } else {
      const repaid = event.repaid_deposit || "0";
      prevRepaidDeposits.set(permIdStr, repaid);
    }
  }

  return {
    ecosystem_slash_events: ecosystemSlashEvents,
    ecosystem_slashed_amount: ecosystemSlashedAmount.toString(),
    ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid.toString(),
    network_slash_events: networkSlashEvents,
    network_slashed_amount: networkSlashedAmount.toString(),
    network_slashed_amount_repaid: networkSlashedAmountRepaid.toString(),
  };
}

async function recalculateAllStats() {
  console.log("🔄 Starting recalculation of permission statistics...\n");

  const environment = process.env.NODE_ENV || "development";
  const config = getConfigForEnv();
  const db = knex(config);

  try {
    const hasWeightColumn = await db.schema.hasColumn("permissions", "weight");
    const hasParticipantsColumn = await db.schema.hasColumn("permissions", "participants");
    const hasIssuedColumn = await db.schema.hasColumn("permissions", "issued");
    const hasVerifiedColumn = await db.schema.hasColumn("permissions", "verified");
    const hasEcosystemSlashEventsColumn = await db.schema.hasColumn("permissions", "ecosystem_slash_events");

    if (!hasWeightColumn || !hasParticipantsColumn || !hasIssuedColumn || !hasVerifiedColumn) {
      console.error("❌ Required columns are missing. Please run migrations first.");
      return;
    }

    const allPermissions = await db("permissions")
      .select("id", "schema_id")
      .orderBy("schema_id")
      .orderBy("id")
      .timeout(60000);

    console.log(`📊 Found ${allPermissions.length} permissions to process\n`);

    const bySchema = new Map<string, string[]>();
    for (const perm of allPermissions) {
      const schemaId = String(perm.schema_id);
      if (!bySchema.has(schemaId)) {
        bySchema.set(schemaId, []);
      }
      bySchema.get(schemaId)!.push(String(perm.id));
    }
    
    allPermissions.length = 0;
    if (global.gc) {
      global.gc();
    }

    let processed = 0;
    let updated = 0;

    for (const [schemaId, permIds] of bySchema.entries()) {
      console.log(`\n📋 Processing schema ${schemaId} (${permIds.length} permissions)...`);
      
      if (global.gc) {
        global.gc();
      }

      console.log("   Calculating weights (bottom-up)...");
      const bottomUpOrder = await getBottomUpOrder(db, schemaId);
      
      let schemaProcessed = 0;
      await db.transaction(async (trx) => {
        for (const permId of bottomUpOrder) {
          if (!permIds.includes(permId)) continue;
          
          const weight = await calculateWeightFromDB(trx, permId, schemaId);
          await trx("permissions")
            .where({ id: permId, schema_id: schemaId })
            .update({ weight })
            .timeout(30000);
          schemaProcessed++;
        }
      });
      processed += schemaProcessed;
      
      if (global.gc) {
        global.gc();
      }

      console.log("   Loading permission sessions (one-time load)...");
      const allSessions = await db("permission_sessions")
        .select("authz")
        .timeout(60000);
      console.log(`   Loaded ${allSessions.length} sessions into memory`);

      console.log("   Calculating participants, issued, verified, slash statistics...");
      const now = new Date();
      const BATCH_SIZE = 50;
      
      for (let i = 0; i < permIds.length; i += BATCH_SIZE) {
        const batch = permIds.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(permIds.length / BATCH_SIZE);
        
        console.log(`   Processing batch ${batchNum}/${totalBatches} (${batch.length} permissions)...`);
        
        for (const permId of batch) {
          const [participants, stats, slashStats] = await Promise.all([
            calculateParticipants(db, permId, schemaId, now),
            calculateIssuedVerified(db, permId, schemaId, allSessions),
            calculateSlashStatistics(db, permId, schemaId).catch((err) => {
              console.warn(`Failed to calculate slash statistics for permission ${permId}:`, err?.message || err);
              return {
                ecosystem_slash_events: 0,
                ecosystem_slashed_amount: "0",
                ecosystem_slashed_amount_repaid: "0",
                network_slash_events: 0,
                network_slashed_amount: "0",
                network_slashed_amount_repaid: "0",
              };
            }),
          ]);

          const updateData: any = {
            participants,
            issued: stats.issued,
            verified: stats.verified,
          };

          if (hasEcosystemSlashEventsColumn) {
            updateData.ecosystem_slash_events = slashStats.ecosystem_slash_events;
            updateData.ecosystem_slashed_amount = slashStats.ecosystem_slashed_amount;
            updateData.ecosystem_slashed_amount_repaid = slashStats.ecosystem_slashed_amount_repaid;
            updateData.network_slash_events = slashStats.network_slash_events;
            updateData.network_slashed_amount = slashStats.network_slashed_amount;
            updateData.network_slashed_amount_repaid = slashStats.network_slashed_amount_repaid;
          }

          await db("permissions")
            .where({ id: permId, schema_id: schemaId })
            .update(updateData)
            .timeout(30000);
          updated++;
        }

        if (global.gc && (i + BATCH_SIZE) % (BATCH_SIZE * 5) === 0) {
          console.log(`   Triggering garbage collection after ${i + BATCH_SIZE} permissions...`);
          global.gc();
        }
      }
      
      allSessions.length = 0;
      
      if (global.gc) {
        global.gc();
      }

      console.log(`   ✓ Schema ${schemaId} completed`);
      
      if (global.gc) {
        global.gc();
      }
    }

    console.log(`\n✅ Recalculation complete!`);
    console.log(`   Processed: ${processed} permissions`);
    console.log(`   Updated: ${updated} permissions`);
    
    const memUsage = process.memoryUsage();
    console.log(`\n📊 Memory usage:`);
    console.log(`   Heap Used: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    console.log(`   Heap Total: ${(memUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`);
    console.log(`   RSS: ${(memUsage.rss / 1024 / 1024).toFixed(2)} MB`);
    
    if (global.gc) {
      console.log("   Running final garbage collection...");
      global.gc();
      const memAfterGC = process.memoryUsage();
      console.log(`   After GC - Heap Used: ${(memAfterGC.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    }
  } catch (error: any) {
    console.error("❌ Error during recalculation:", error);
    throw error;
  } finally {
    if (db) {
      await db.destroy();
    }
    
    if (global.gc) {
      global.gc();
    }
  }
}

if (require.main === module) {
  recalculateAllStats()
    .then(() => {
      console.log("\n✨ Done!");
      process.exit(0);
    })
    .catch((error) => {
      console.error("\n💥 Failed:", error);
      process.exit(1);
    });
}

export { recalculateAllStats };
