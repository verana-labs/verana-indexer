import { spawn } from 'node:child_process'
import { readdirSync } from 'node:fs'
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { Config } from '@jest/types'
import knexFactory, { type Knex } from 'knex'
import { getConfigForEnv } from '../../src/knexfile'

const PROBE_TIMEOUT_MS = 5000
const MAINTENANCE_DATABASE = 'postgres'
const UNDEFINED_DATABASE = '3D000'
const COMPOSE_FILE = 'docker/docker-compose.yml'
const COMPOSE_SERVICES = ['psql', 'redis']
const SERVICE_BOOT_TIMEOUT_MS = 90_000

type Connection = { host?: string; port?: number; user?: string; database?: string }
type ProbeResult = 'ready' | 'missing-database' | 'unreachable'

function connectionOf(config: Knex.Config): Connection {
  return (config.connection ?? {}) as Connection
}

function describeTarget(connection: Connection): string {
  return `${connection.host}:${connection.port} as ${connection.user}`
}

function shortLivedClient(config: Knex.Config, database: string): Knex {
  return knexFactory({
    client: config.client,
    connection: { ...connectionOf(config), database },
    pool: {
      min: 0,
      max: 1,
      acquireTimeoutMillis: PROBE_TIMEOUT_MS,
      createTimeoutMillis: PROBE_TIMEOUT_MS,
      propagateCreateError: true,
    },
  })
}

function run(command: string, args: string[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      env: { ...process.env, NODE_ENV: 'test' },
      stdio: 'inherit',
      shell: process.platform === 'win32',
    })
    child.on('error', reject)
    child.on('exit', (code) => resolve(code ?? 1))
  })
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function probe(config: Knex.Config, database: string): Promise<ProbeResult> {
  const db = shortLivedClient(config, database)
  try {
    await db.raw('SELECT 1')
    return 'ready'
  } catch (error: any) {
    return error?.code === UNDEFINED_DATABASE ? 'missing-database' : 'unreachable'
  } finally {
    await db.destroy().catch(() => undefined)
  }
}

/**
 * Brings up the services the suite depends on. They are left running afterwards so consecutive runs reuse them
 * (starting Postgres and replaying migrations on every run would turn a two-second suite into a minute one).
 * Stop them with `pnpm run stop` when you are done.
 */
async function startServices(connection: Connection): Promise<void> {
  console.log(`No PostgreSQL at ${describeTarget(connection)}. Starting ${COMPOSE_SERVICES.join(' and ')}...`)

  const code = await run('docker', ['compose', '-f', COMPOSE_FILE, 'up', '-d', ...COMPOSE_SERVICES]).catch(() => 1)
  if (code !== 0) {
    throw new Error(
      `Could not start the test services with Docker.\n` +
        `Start them yourself with: docker compose -f ${COMPOSE_FILE} up -d ${COMPOSE_SERVICES.join(' ')}\n` +
        `Or point the suite at an existing PostgreSQL through POSTGRES_HOST / POSTGRES_PORT.`
    )
  }
}

async function waitForPostgres(config: Knex.Config, database: string): Promise<ProbeResult> {
  const startedAt = Date.now()
  while (Date.now() - startedAt < SERVICE_BOOT_TIMEOUT_MS) {
    const result = await probe(config, database)
    if (result !== 'unreachable') return result
    await sleep(2000)
  }
  throw new Error(
    `PostgreSQL did not accept connections within ${SERVICE_BOOT_TIMEOUT_MS / 1000}s.\n` +
      `Check the container logs with: docker compose -f ${COMPOSE_FILE} logs psql`
  )
}

/**
 * Creating the database is idempotent on purpose: the image's `docker-entrypoint-initdb.d` hook only fires on a
 * virgin data volume, so any environment whose volume predates it has no test database.
 */
async function ensureTestDatabase(config: Knex.Config, state: ProbeResult): Promise<void> {
  if (state !== 'missing-database') return

  const database = String(connectionOf(config).database)
  const admin = shortLivedClient(config, MAINTENANCE_DATABASE)
  try {
    console.log(`Creating missing test database "${database}"...`)
    await admin.raw('CREATE DATABASE ??', [database])
  } finally {
    await admin.destroy().catch(() => undefined)
  }
}

/**
 * Compares migration filenames against `knex_migrations` instead of calling `migrate.list()`: the migrations are
 * TypeScript, and knex would load them through a plain require that Jest's transform does not cover.
 */
async function needsMigrations(config: Knex.Config): Promise<boolean> {
  const migrations = config.migrations as { directory?: string; loadExtensions?: string[] } | undefined
  const directory = migrations?.directory ?? ''
  const extensions = migrations?.loadExtensions ?? ['.ts']
  const onDisk = readdirSync(directory).filter((file: string) =>
    extensions.some((extension) => file.endsWith(extension))
  )

  const db = shortLivedClient(config, String(connectionOf(config).database))
  try {
    if (!(await db.schema.hasTable('knex_migrations'))) return true
    if (!(await db.schema.hasTable('block'))) return true
    const applied = new Set((await db('knex_migrations').select('name')).map((row: { name: string }) => row.name))
    return onDisk.some((file: string) => !applied.has(file))
  } catch {
    return true
  } finally {
    await db.destroy().catch(() => undefined)
  }
}

async function runMigrations(): Promise<void> {
  console.log('Applying migrations to the test database (only happens when the schema is behind)...')
  const code = await run('pnpm', ['run', 'migrate:dev'])
  if (code !== 0) throw new Error(`Migrations failed with exit code ${code}`)
}

export default async function setUp(_globalConfig: Config.GlobalConfig, _projectConfig: Config.ProjectConfig) {
  // Set before anything reads it, so workers forked after this inherit the test connection.
  process.env.NODE_ENV = 'test'

  const config = getConfigForEnv('test')
  const connection = connectionOf(config)
  const database = String(connection.database ?? '')
  if (!database) throw new Error('No test database configured. Set POSTGRES_DB_TEST.')

  console.log(`Preparing test database "${database}" at ${describeTarget(connection)}...`)

  // A reachable database — the CI path — needs no Docker and no maintenance connection.
  let state = await probe(config, database)
  if (state === 'unreachable') {
    await startServices(connection)
    state = await waitForPostgres(config, database)
  }

  await ensureTestDatabase(config, state)
  if (await needsMigrations(config)) await runMigrations()

  console.log('✅ Test database is ready.')
}
