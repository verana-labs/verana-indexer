/* eslint-disable no-console */
import * as dotenv from 'dotenv'
import * as fs from 'fs'
import knex, { Knex } from 'knex'
import * as path from 'path'
import { loadEnvFiles } from '../common/utils/loadEnv'
import { getConfigForEnv } from '../knexfile'
import { resetForReindex } from './reindex-tables'

loadEnvFiles()
dotenv.config()

async function waitForDatabase(config: Knex.Config, maxRetries = 30, delayMs = 2000): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    let testDb: Knex | undefined
    try {
      const testConfig: Knex.Config = {
        client: config.client,
        connection: config.connection,
        pool: {
          min: 0,
          max: 10,
          acquireTimeoutMillis: 10000,
          createTimeoutMillis: 10000,
          idleTimeoutMillis: 1000,
          reapIntervalMillis: 1000,
          destroyTimeoutMillis: 5000,
          propagateCreateError: false,
        },
      }

      testDb = knex(testConfig)
      await testDb.raw('SELECT 1')
      await testDb.destroy().catch(() => undefined)
      return
    } catch (error: unknown) {
      if (testDb) {
        await testDb.destroy().catch(() => undefined)
      }

      const err = error as NodeJS.ErrnoException
      if (i === maxRetries - 1) {
        const errorMsg = err?.message || String(error)
        const errorCode = err?.code || 'UNKNOWN'
        throw new Error(
          `Database not ready after ${maxRetries} attempts.\n` + `Error: ${errorMsg}\n` + `Code: ${errorCode}`
        )
      }

      const errorMsg = err?.message || String(error)
      const shortMsg = errorMsg.length > 100 ? `${errorMsg.substring(0, 100)}...` : errorMsg
      console.log(`Waiting for database... (attempt ${i + 1}/${maxRetries}) - ${shortMsg}`)
      await new Promise<void>((resolve) => {
        setTimeout(resolve, delayMs)
      })
    }
  }
}

async function removeErrorsLog(): Promise<void> {
  const logPath = path.join(process.cwd(), 'logs', 'errors.log')
  try {
    await fs.promises.unlink(logPath)
    console.log(' Removed logs/errors.log')
  } catch (err: unknown) {
    const error = err as NodeJS.ErrnoException
    if (error?.code === 'ENOENT') {
      console.log(' logs/errors.log not found; nothing to remove')
    } else {
      console.warn(` Could not remove logs/errors.log: ${error?.message || String(error)}`)
    }
  }
}

async function checkTableExists(db: Knex, tableName: string): Promise<boolean> {
  const result = await db.raw(
    `
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = ?
    );
  `,
    [tableName]
  )
  return result.rows[0]?.exists || false
}

const REINDEX_APP_NAME = 'reindex-prepare'

async function acquireExclusiveDbAccess(db: Knex): Promise<void> {
  console.log('  Acquiring exclusive database access...')

  // 1. Tag our connections so we can identify them
  await db.raw(`SET application_name = '${REINDEX_APP_NAME}'`)

  // 2. Restrict new connections to the database.
  //    Existing connections (ours) stay alive; the old indexer's pool can't
  //    create NEW connections once its current ones are terminated.
  //    We allow a small number (our pool size) rather than 0 to avoid blocking ourselves.
  const dbNameResult = await db.raw(`SELECT current_database() AS db`)
  const dbName = dbNameResult.rows[0]?.db
  try {
    await db.raw(`ALTER DATABASE "${dbName}" CONNECTION LIMIT 5`)
    console.log(`  Database "${dbName}" connection limit set to 5`)
  } catch (alterErr: unknown) {
    const error = alterErr as NodeJS.ErrnoException
    console.warn(`  Warning: Could not restrict connection limit: ${error.message}`)
    console.warn(`  The DB user may not own the database. Falling back to terminate-only.`)
  }

  // 3. Terminate all connections that aren't ours.
  //    The old indexer's pool will try to reconnect but hit the connection limit.
  const terminated = await db.raw(`
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid != pg_backend_pid()
      AND (application_name IS NULL OR application_name != '${REINDEX_APP_NAME}')
  `)
  const count = terminated.rows?.length || 0
  console.log(`  Terminated ${count} other connection(s)`)

  // Brief pause to let PostgreSQL clean up terminated backends
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 1000)
  })

  // 4. Verify we have exclusive access
  const remaining = await db.raw(`
    SELECT pid, application_name, state, query
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid != pg_backend_pid()
      AND (application_name IS NULL OR application_name != '${REINDEX_APP_NAME}')
  `)
  if (remaining.rows?.length > 0) {
    console.warn(`  Warning: ${remaining.rows.length} non-reindex connection(s) still active`)
    // Terminate stragglers
    await db.raw(`
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid != pg_backend_pid()
        AND (application_name IS NULL OR application_name != '${REINDEX_APP_NAME}')
    `)
  } else {
    console.log('  Exclusive database access confirmed')
  }
}

async function releaseExclusiveDbAccess(db: Knex): Promise<void> {
  try {
    const dbNameResult = await db.raw(`SELECT current_database() AS db`)
    const dbName = dbNameResult.rows[0]?.db
    await db.raw(`ALTER DATABASE "${dbName}" CONNECTION LIMIT -1`)
    console.log('  Database connection limit restored to unlimited')
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException
    console.warn(`  Warning: Could not restore connection limit: ${err.message}`)
    console.warn(`  You may need to run: ALTER DATABASE <dbname> CONNECTION LIMIT -1;`)
  }
}

async function handleGenesisFile(): Promise<void> {
  console.log('Step 3: Handling genesis.json file...')

  const genesisPath = path.resolve('genesis.json')
  const genesisBackupPath = path.resolve('genesis.json.backup')

  try {
    if (fs.existsSync(genesisPath)) {
      fs.copyFileSync(genesisPath, genesisBackupPath)
      console.log(`   Backed up genesis.json to genesis.json.backup`)

      fs.unlinkSync(genesisPath)
      console.log(`   Removed genesis.json (will be re-fetched on next start)`)
      console.log(`   To restore: cp genesis.json.backup genesis.json`)
    } else {
      console.log(`   genesis.json not found, skipping`)
    }
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException
    console.warn(`    Error handling genesis.json: ${err.message}`)
  }

  console.log(' Genesis file handled successfully\n')
}

async function verifyBlocksTable(db: Knex): Promise<void> {
  console.log('Verifying block table is preserved...')

  const blocksExists = await checkTableExists(db, 'block')
  if (!blocksExists) {
    throw new Error('CRITICAL: block table does not exist! This should never be dropped.')
  }

  const blockCount = await db('block').count('* as count').first()
  console.log(`   Block table exists with ${blockCount?.count || 0} rows`)
  console.log(' Block table verified\n')
}

;(async function reindexModules(): Promise<void> {
  const environment = process.env.NODE_ENV || 'production'
  process.env.NODE_ENV = environment
  process.env.MIGRATION_MODE = 'lightweight'

  if (environment === 'test') {
    console.error('ERROR: Reindex script cannot run in test mode.')
    console.error('Reindexing is only for development and production environments.')
    console.error('Test environment should only run migrations, not reindex operations.')
    process.exit(1)
  }

  let db: Knex | undefined

  function logMemoryUsage(stage: string) {
    if (global.gc) {
      global.gc()
    }
    const memUsage = process.memoryUsage()
    const formatMB = (bytes: number) => (bytes / 1024 / 1024).toFixed(2)
    console.log(`\n[Memory ${stage}]`)
    console.log(`  Heap Used: ${formatMB(memUsage.heapUsed)} MB`)
    console.log(`  Heap Total: ${formatMB(memUsage.heapTotal)} MB`)
    console.log(`  External: ${formatMB(memUsage.external)} MB`)
    console.log(`  RSS: ${formatMB(memUsage.rss)} MB\n`)
  }

  const checkpointFile = path.join(process.cwd(), '.reindex-checkpoint.json')
  let checkpointData: {
    completedSteps: string[]
    lastCompletedStep?: string
    attemptCount?: number
    timestamp?: string
  } | null = null

  try {
    if (process.env.REINDEX_CHECKPOINT) {
      checkpointData = JSON.parse(process.env.REINDEX_CHECKPOINT)
    } else if (fs.existsSync(checkpointFile)) {
      const content = fs.readFileSync(checkpointFile, 'utf-8')
      checkpointData = JSON.parse(content)
    }
  } catch (error) {
    console.warn(`⚠️  Could not load checkpoint: ${error}`)
  }

  if (!checkpointData) {
    checkpointData = { completedSteps: [], attemptCount: 0 }
  }

  const completedSteps: string[] = checkpointData.completedSteps || []

  function markStepComplete(stepName: string): void {
    if (!checkpointData) return
    if (!completedSteps.includes(stepName)) {
      completedSteps.push(stepName)
      checkpointData.completedSteps = completedSteps
      checkpointData.lastCompletedStep = stepName
      checkpointData.timestamp = new Date().toISOString()
      process.env.REINDEX_CHECKPOINT = JSON.stringify(checkpointData)

      try {
        fs.writeFileSync(checkpointFile, JSON.stringify(checkpointData, null, 2))
      } catch (error) {
        console.warn(`⚠️  Could not save checkpoint to file: ${error}`)
      }
    }
  }

  function isStepComplete(stepName: string): boolean {
    return completedSteps.includes(stepName)
  }

  try {
    logMemoryUsage('Start')
    console.log(' Starting Module Reindexing Process')
    console.log(`Environment: ${environment}\n`)

    if (checkpointData && completedSteps.length > 0) {
      console.log(`📌 Resuming from checkpoint. Completed steps: ${completedSteps.join(', ')}\n`)
    }

    if (!isStepComplete('remove-errors-log')) {
      console.log('Step 0: Removing errors.log...')
      await removeErrorsLog()
      markStepComplete('remove-errors-log')
      console.log('')
    } else {
      console.log('Step 0: Removing errors.log... [SKIPPED - already completed]\n')
    }

    const config = getConfigForEnv()

    const connInfo = config.connection as { database?: string; host?: string; port?: number }
    console.log(
      `Database: ${connInfo.database || 'unknown'} @ ${connInfo.host || 'unknown'}:${connInfo.port || 'unknown'}\n`
    )

    if (!isStepComplete('connect')) {
      console.log('Step 1: Connecting to database...')
      await waitForDatabase(config)
      console.log(' Database connection established\n')
      markStepComplete('connect')
    } else {
      console.log('Step 1: Connecting to database... [SKIPPED - already completed]\n')
    }

    // Override pool to stay within the CONNECTION LIMIT we'll set (5).
    // Tag all connections with our app name so acquireExclusiveDbAccess
    // can distinguish them from the old indexer's connections.
    const reindexConfig: Knex.Config = {
      ...config,
      pool: {
        min: 1,
        max: 3,
        acquireTimeoutMillis: 120000,
        createTimeoutMillis: 30000,
        destroyTimeoutMillis: 5000,
        idleTimeoutMillis: 5000,
        reapIntervalMillis: 500,
        propagateCreateError: false,
        afterCreate: (conn: any, done: (err: Error | null, conn: any) => void) => {
          conn.query(`SET application_name = '${REINDEX_APP_NAME}'`, (err: Error | null) => done(err, conn))
        },
      },
    }
    db = knex(reindexConfig)

    if (!isStepComplete('verify-blocks')) {
      await verifyBlocksTable(db)
      markStepComplete('verify-blocks')
    } else {
      console.log('Step: Verify blocks table... [SKIPPED - already completed]\n')
    }

    if (!isStepComplete('exclusive-access')) {
      console.log('\nStep 1.5: Acquiring exclusive database access...')
      await acquireExclusiveDbAccess(db)
      markStepComplete('exclusive-access')
    } else {
      // Re-acquire on resume — the connection limit may have been restored by a crash
      console.log('Step 1.5: Re-acquiring exclusive DB access (resuming)...')
      await acquireExclusiveDbAccess(db)
    }

    // A new step name: a resume file left by the older script must not skip this step.
    if (!isStepComplete('reset-data')) {
      console.log('\nStep 2: Emptying the tables that are rebuilt from blocks and resetting the checkpoints...')
      const { tables, highestBlock } = await resetForReindex(db)
      console.log(`  Emptied ${tables.length} tables. The schema and the block data are unchanged.`)
      console.log(`  Every job starts again from 0. The block crawler continues after block ${highestBlock}.\n`)
      markStepComplete('reset-data')
    } else {
      console.log('Step 2: Reset data... [SKIPPED - already completed]\n')
    }

    if (!isStepComplete('genesis')) {
      await handleGenesisFile()
      markStepComplete('genesis')
    } else {
      console.log('Step: Handle genesis file... [SKIPPED - already completed]\n')
    }

    logMemoryUsage('End')

    try {
      if (fs.existsSync(checkpointFile)) {
        fs.unlinkSync(checkpointFile)
        console.log(' ✅ Checkpoint file cleared\n')
      }
    } catch (error) {
      console.warn(` ⚠️  Could not clear checkpoint file: ${error}\n`)
    }

    console.log(' Reindexing preparation completed successfully!')
    console.log('\n Next steps:')
    console.log('  1. Start the indexer services')
    console.log('  2. Services will process existing blocks from the database')
    console.log('  3. Block crawler will only fetch new blocks (not existing ones)')
    console.log('  4. Monitor checkpoint progress')
    console.log('\n Optimization tips:')
    console.log('  - During reindexing, services process existing blocks faster')
    console.log('  - Block crawler skips fetching blocks that already exist')
    console.log('  - After reindexing completes, normal block crawling resumes')
    console.log('\nTo start indexer:')
    console.log('  Development: pnpm run dev')
    console.log('  Production:  pnpm run start')
  } catch (error: unknown) {
    const err = error as Error
    logMemoryUsage('Error')
    console.error('\n❌ Reindexing failed:', err.message)
    if (err.stack) {
      console.error(err.stack)
    }

    if (
      err.message?.includes('heap') ||
      err.message?.includes('memory') ||
      err.message?.includes('Allocation failed')
    ) {
      console.error('\n⚠️  Memory error detected. The process will be restarted automatically.')
      console.error('   If this persists, try:')
      console.error("   - Increasing NODE_OPTIONS='--max-old-space-size=12288'")
      console.error('   - Processing data in smaller batches')
    }

    process.exitCode = 1
  } finally {
    if (db) {
      await releaseExclusiveDbAccess(db).catch(() => undefined)
      await db.destroy().catch(() => undefined)
    }
    process.exit(process.exitCode || 0)
  }
})()
