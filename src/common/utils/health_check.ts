import os from 'os';
import knex from './db_connection';

export interface HealthStatus {
  database: {
    healthy: boolean;
    activeConnections?: number;
    maxConnections?: number;
    connectionUsagePercent?: number;
    poolUsed?: number;
    poolFree?: number;
    poolPending?: number;
  };
  server: {
    healthy: boolean;
    cpuUsagePercent?: number;
    memoryUsagePercent?: number;
    freeMemoryMB?: number;
    heapUsedMB?: number;
    heapTotalMB?: number;
  };
  overall: 'healthy' | 'degraded' | 'critical';
  timestamp: string;
}

let lastHealthCheck: HealthStatus | null = null;
let lastHealthCheckTime: number = 0;
const HEALTH_CHECK_CACHE_MS = 5000;

export async function checkHealth(): Promise<HealthStatus> {
  const now = Date.now();
  if (lastHealthCheck && (now - lastHealthCheckTime) < HEALTH_CHECK_CACHE_MS) {
    return lastHealthCheck;
  }

  const health: HealthStatus = {
    database: { healthy: false },
    server: { healthy: false },
    overall: 'critical',
    timestamp: new Date().toISOString()
  };

  // Get Knex pool stats (doesn't require DB query)
  let poolUsed = 0;
  let poolFree = 0;
  let poolPending = 0;

  try {
    if (knex.client && (knex.client as any).pool) {
      const pool = (knex.client as any).pool;
      poolUsed = pool.used?.length || 0;
      poolFree = pool.free?.length || 0;
      poolPending = pool.pendingAcquires?.length || 0;
    }
  } catch (poolError) {
    // Pool stats not available, continue
  }

  try {
    const dbStats = await knex.raw(`
      SELECT
        count(*) as active,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max
      FROM pg_stat_activity
      WHERE datname = current_database()
    `);

    const activeConnections = parseInt(dbStats.rows[0]?.active || '0', 10);
    const maxConnections = parseInt(dbStats.rows[0]?.max || '100', 10);
    const connectionUsagePercent = (activeConnections / maxConnections) * 100;

    // Database is healthy if connection usage is below 80% AND no excessive pending acquires
    const isHealthy = connectionUsagePercent < 80 && poolPending < 50;

    health.database = {
      healthy: isHealthy,
      activeConnections,
      maxConnections,
      connectionUsagePercent,
      poolUsed,
      poolFree,
      poolPending
    };
  } catch (error) {
    // If DB query fails, check if it's just slow or actually down
    health.database = {
      healthy: false,
      poolUsed,
      poolFree,
      poolPending
    };
  }

  try {
    const totalMemory = os.totalmem();
    const freeMemory = os.freemem();
    const usedMemory = totalMemory - freeMemory;
    const memoryUsagePercent = (usedMemory / totalMemory) * 100;
    const freeMemoryMB = freeMemory / (1024 * 1024);

    // Get Node.js heap memory usage
    const heapUsed = process.memoryUsage().heapUsed;
    const heapTotal = process.memoryUsage().heapTotal;
    const heapUsedMB = heapUsed / (1024 * 1024);
    const heapTotalMB = heapTotal / (1024 * 1024);

    const cpuUsage = process.cpuUsage();
    const cpuUsagePercent = Math.min(100, (cpuUsage.user + cpuUsage.system) / 1000000);

    // Server is healthy if:
    // - System memory usage < 85%
    // - Free system memory > 500MB
    // - Heap used < 90% of heap total
    const heapUsagePercent = (heapUsed / heapTotal) * 100;
    const isHealthy = memoryUsagePercent < 85 && freeMemoryMB > 500 && heapUsagePercent < 90;

    health.server = {
      healthy: isHealthy,
      cpuUsagePercent,
      memoryUsagePercent,
      freeMemoryMB,
      heapUsedMB: Math.round(heapUsedMB * 100) / 100,
      heapTotalMB: Math.round(heapTotalMB * 100) / 100
    };
  } catch (error) {
    health.server.healthy = false;
  }

  if (health.database.healthy && health.server.healthy) {
    health.overall = 'healthy';
  } else if (health.database.healthy || health.server.healthy) {
    health.overall = 'degraded';
  } else {
    health.overall = 'critical';
  }

  lastHealthCheck = health;
  lastHealthCheckTime = now;

  return health;
}

/**
 * Lightweight health check that doesn't hit the database
 * Use this for Kubernetes liveness probes
 */
export function checkLiveness(): { status: 'ok' | 'error'; timestamp: string } {
  return {
    status: 'ok',
    timestamp: new Date().toISOString()
  };
}

/**
 * Quick readiness check using cached data
 * Use this for Kubernetes readiness probes
 */
export function checkReadinessQuick(): { ready: boolean; timestamp: string; reason?: string } {
  // Use cached health check if available
  if (lastHealthCheck) {
    return {
      ready: lastHealthCheck.overall !== 'critical',
      timestamp: new Date().toISOString(),
      reason: lastHealthCheck.overall === 'critical' ? 'Health check critical' : undefined
    };
  }

  // No cached data yet, assume ready (health check will run shortly)
  return {
    ready: true,
    timestamp: new Date().toISOString()
  };
}

export function getOptimalBlocksPerCall(
  baseBlocksPerCall: number,
  health: HealthStatus,
  isFreshStart: boolean
): number {
  if (isFreshStart) {
    if (health.overall === 'critical') {
      return Math.max(10, Math.floor(baseBlocksPerCall * 0.1));
    }
    if (health.overall === 'degraded') {
      return Math.max(50, Math.floor(baseBlocksPerCall * 0.3));
    }
    return Math.max(100, Math.floor(baseBlocksPerCall * 0.5));
  }
  
  if (health.overall === 'critical') {
    return Math.max(100, Math.floor(baseBlocksPerCall * 0.5));
  }
  if (health.overall === 'degraded') {
    return Math.max(500, Math.floor(baseBlocksPerCall * 0.7));
  }
  return baseBlocksPerCall;
}

export function getOptimalDelay(
  baseDelay: number,
  health: HealthStatus,
  isFreshStart: boolean
): number {
  if (isFreshStart) {
    if (health.overall === 'critical') {
      return baseDelay * 5;
    }
    if (health.overall === 'degraded') {
      return baseDelay * 3;
    }
    return baseDelay * 2;
  }
  
  if (health.overall === 'critical') {
    return baseDelay * 2;
  }
  if (health.overall === 'degraded') {
    return baseDelay * 1.5;
  }
  return baseDelay;
}

