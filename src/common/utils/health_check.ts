import os from 'os';
import knex from './db_connection';

export interface HealthStatus {
  database: {
    healthy: boolean;
    activeConnections?: number;
    maxConnections?: number;
    connectionUsagePercent?: number;
  };
  server: {
    healthy: boolean;
    cpuUsagePercent?: number;
    memoryUsagePercent?: number;
    freeMemoryMB?: number;
  };
  overall: 'healthy' | 'degraded' | 'critical';
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
    overall: 'critical'
  };

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
    
    health.database = {
      healthy: connectionUsagePercent < 80,
      activeConnections,
      maxConnections,
      connectionUsagePercent
    };
  } catch (error) {
    health.database.healthy = false;
  }

  try {
    const totalMemory = os.totalmem();
    const freeMemory = os.freemem();
    const usedMemory = totalMemory - freeMemory;
    const memoryUsagePercent = (usedMemory / totalMemory) * 100;
    const freeMemoryMB = freeMemory / (1024 * 1024);
    
    const cpuUsage = process.cpuUsage();
    const cpuUsagePercent = Math.min(100, (cpuUsage.user + cpuUsage.system) / 1000000);
    
    health.server = {
      healthy: memoryUsagePercent < 85 && freeMemoryMB > 500,
      cpuUsagePercent,
      memoryUsagePercent,
      freeMemoryMB
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

