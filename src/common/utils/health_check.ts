import os from 'os';
import fs from 'fs';
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
    heapUsagePercent?: number;
    heapUsedMB?: number;
    heapTotalMB?: number;
  };
  overall: 'healthy' | 'degraded' | 'critical';
}

let lastHealthCheck: HealthStatus | null = null;
let lastHealthCheckTime: number = 0;
let healthCheckPromise: Promise<HealthStatus> | null = null; // Prevent race conditions
const HEALTH_CHECK_CACHE_MS = 3000; // Reduced from 5000 for more responsive checks

// Memory thresholds - more aggressive to prevent OOM
const MEMORY_HEALTHY_THRESHOLD = 70;      // Below 70% = healthy
const MEMORY_DEGRADED_THRESHOLD = 80;     // 70-80% = degraded
const MEMORY_CRITICAL_THRESHOLD = 85;     // Above 80% = critical (was 85%)
const HEAP_HEALTHY_THRESHOLD = 65;        // Node.js heap thresholds
const HEAP_CRITICAL_THRESHOLD = 75;

/**
 * Safe parseInt that returns a default value if parsing fails or results in NaN
 */
function safeParseInt(value: string | undefined | null, defaultValue: number): number {
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  return Number.isNaN(parsed) || !Number.isFinite(parsed) ? defaultValue : parsed;
}

/**
 * Safe percentage calculation that handles division by zero
 */
function safePercentage(numerator: number, denominator: number): number {
  if (denominator === 0) return 0;
  const result = (numerator / denominator) * 100;
  return Number.isFinite(result) ? Math.max(0, Math.min(100, result)) : 0;
}

/**
 * Get container memory limit from cgroup (works in Kubernetes/Docker)
 * Falls back to OS memory if not in a container
 */
function getContainerMemoryLimit(): number {
  // Skip cgroup checks on non-Linux platforms (Windows, macOS)
  if (os.platform() !== 'linux') {
    return os.totalmem();
  }

  try {
    // Try cgroup v2 first (newer systems)
    if (fs.existsSync('/sys/fs/cgroup/memory.max')) {
      const maxStr = fs.readFileSync('/sys/fs/cgroup/memory.max', 'utf8').trim();
      if (maxStr !== 'max' && maxStr !== 'infinity') {
        const limit = safeParseInt(maxStr, os.totalmem());
        if (limit > 0) return limit;
      }
    }
    // Try cgroup v1 (older systems)
    if (fs.existsSync('/sys/fs/cgroup/memory/memory.limit_in_bytes')) {
      const limitStr = fs.readFileSync('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'utf8').trim();
      const limit = safeParseInt(limitStr, os.totalmem());
      // If limit is very high (close to system max), it's effectively unlimited
      if (limit > 0 && limit < os.totalmem() * 0.95) {
        return limit;
      }
    }
  } catch {
    // Ignore errors, fall back to OS memory
  }
  return os.totalmem();
}

/**
 * Get current container memory usage from cgroup
 * Falls back to OS memory calculation if not in a container
 */
function getContainerMemoryUsage(): number {
  // Skip cgroup checks on non-Linux platforms
  if (os.platform() !== 'linux') {
    return Math.max(0, os.totalmem() - os.freemem());
  }

  try {
    // Try cgroup v2 first
    if (fs.existsSync('/sys/fs/cgroup/memory.current')) {
      const usage = safeParseInt(
        fs.readFileSync('/sys/fs/cgroup/memory.current', 'utf8').trim(),
        os.totalmem() - os.freemem()
      );
      if (usage > 0) return usage;
    }
    // Try cgroup v1
    if (fs.existsSync('/sys/fs/cgroup/memory/memory.usage_in_bytes')) {
      const usage = safeParseInt(
        fs.readFileSync('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'utf8').trim(),
        os.totalmem() - os.freemem()
      );
      if (usage > 0) return usage;
    }
  } catch {
    // Ignore errors, fall back to calculation
  }
  return Math.max(0, os.totalmem() - os.freemem());
}

/**
 * Trigger garbage collection if available
 */
export function triggerGC(): boolean {
  if (global.gc) {
    global.gc();
    return true;
  }
  return false;
}

export async function checkHealth(): Promise<HealthStatus> {
  const now = Date.now();

  // Return cached result if valid
  if (lastHealthCheck && (now - lastHealthCheckTime) < HEALTH_CHECK_CACHE_MS) {
    return lastHealthCheck;
  }

  // Return in-flight request if one exists (prevents race conditions)
  if (healthCheckPromise) {
    return healthCheckPromise;
  }

  // Create new health check promise
  healthCheckPromise = performHealthCheck();

  try {
    const result = await healthCheckPromise;
    lastHealthCheck = result;
    lastHealthCheckTime = Date.now();
    return result;
  } finally {
    healthCheckPromise = null;
  }
}

async function performHealthCheck(): Promise<HealthStatus> {
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

    const activeConnections = safeParseInt(dbStats.rows[0]?.active, 0);
    const maxConnections = safeParseInt(dbStats.rows[0]?.max, 100);
    const connectionUsagePercent = safePercentage(activeConnections, maxConnections);

    health.database = {
      healthy: connectionUsagePercent < 80,
      activeConnections,
      maxConnections,
      connectionUsagePercent
    };
  } catch (error) {
    // Log error for debugging but don't fail the health check entirely
    console.error('Health check database query failed:', error);
    health.database.healthy = false;
  }

  try {
    // Use container-aware memory metrics
    const totalMemory = Math.max(1, getContainerMemoryLimit()); // Prevent division by zero
    const usedMemory = Math.max(0, getContainerMemoryUsage());
    const freeMemory = Math.max(0, totalMemory - usedMemory);
    const memoryUsagePercent = safePercentage(usedMemory, totalMemory);
    const freeMemoryMB = freeMemory / (1024 * 1024);

    // Also check Node.js heap memory (more accurate for detecting JS memory leaks)
    const heapStats = process.memoryUsage();
    const heapUsedMB = Math.max(0, heapStats.heapUsed / (1024 * 1024));
    const heapTotalMB = Math.max(1, heapStats.heapTotal / (1024 * 1024));
    const heapUsagePercent = safePercentage(heapStats.heapUsed, heapStats.heapTotal);

    // Note: process.cpuUsage() returns cumulative time, not current usage percentage
    // We track it but it's not a reliable instant CPU metric
    const cpuUsage = process.cpuUsage();
    const cpuUsagePercent = 0; // Set to 0 as cumulative time isn't useful here

    // Server is healthy only if BOTH container memory and heap are in good shape
    const containerMemoryHealthy = memoryUsagePercent < MEMORY_HEALTHY_THRESHOLD && freeMemoryMB > 200;
    const heapHealthy = heapUsagePercent < HEAP_HEALTHY_THRESHOLD;

    health.server = {
      healthy: containerMemoryHealthy && heapHealthy,
      cpuUsagePercent,
      memoryUsagePercent,
      freeMemoryMB,
      heapUsagePercent,
      heapUsedMB,
      heapTotalMB
    };

    // Trigger GC if memory is getting high (proactive cleanup)
    if (memoryUsagePercent > MEMORY_DEGRADED_THRESHOLD || heapUsagePercent > HEAP_CRITICAL_THRESHOLD) {
      triggerGC();
    }
  } catch (error) {
    console.error('Health check server metrics failed:', error);
    health.server.healthy = false;
  }

  // Determine overall health with more conservative thresholds
  const memoryPercent = health.server.memoryUsagePercent || 100;
  const heapPercent = health.server.heapUsagePercent || 100;
  const dbUsagePercent = health.database.connectionUsagePercent || 100;

  // Critical if ANY of these conditions are met (more aggressive)
  const isCritical = memoryPercent >= MEMORY_CRITICAL_THRESHOLD ||
                     heapPercent >= HEAP_CRITICAL_THRESHOLD ||
                     dbUsagePercent >= 95;

  // Degraded if approaching limits
  const isDegraded = memoryPercent >= MEMORY_HEALTHY_THRESHOLD ||
                     heapPercent >= HEAP_HEALTHY_THRESHOLD ||
                     dbUsagePercent >= 80;

  if (isCritical) {
    health.overall = 'critical';
  } else if (isDegraded) {
    health.overall = 'degraded';
  } else if (health.database.healthy && health.server.healthy) {
    health.overall = 'healthy';
  } else {
    health.overall = 'degraded';
  }

  return health;
}

export function getOptimalBlocksPerCall(
  baseBlocksPerCall: number,
  health: HealthStatus,
  isFreshStart: boolean
): number {
  const memoryPercent = health.server.memoryUsagePercent || 0;
  const heapPercent = health.server.heapUsagePercent || 0;

  // Emergency mode: drastically reduce if memory is extremely high
  if (memoryPercent > 90 || heapPercent > 85) {
    return Math.max(5, Math.floor(baseBlocksPerCall * 0.05)); // Only 5% of base
  }

  if (isFreshStart) {
    if (health.overall === 'critical') {
      return Math.max(5, Math.floor(baseBlocksPerCall * 0.1));  // 10% of base
    }
    if (health.overall === 'degraded') {
      return Math.max(10, Math.floor(baseBlocksPerCall * 0.2)); // 20% of base
    }
    return Math.max(25, Math.floor(baseBlocksPerCall * 0.4));   // 40% of base when healthy
  }

  // Reindexing mode - more aggressive throttling
  if (health.overall === 'critical') {
    return Math.max(25, Math.floor(baseBlocksPerCall * 0.1));   // 10% of base (was 50%)
  }
  if (health.overall === 'degraded') {
    return Math.max(50, Math.floor(baseBlocksPerCall * 0.25));  // 25% of base (was 70%)
  }
  // Even when healthy, cap the blocks per call to prevent memory spikes
  return Math.min(baseBlocksPerCall, 500);
}

export function getOptimalDelay(
  baseDelay: number,
  health: HealthStatus,
  isFreshStart: boolean
): number {
  const memoryPercent = health.server.memoryUsagePercent || 0;
  const heapPercent = health.server.heapUsagePercent || 0;

  // Emergency mode: add significant delay if memory is extremely high
  if (memoryPercent > 90 || heapPercent > 85) {
    return Math.max(baseDelay * 10, 5000); // At least 5 seconds
  }

  if (isFreshStart) {
    if (health.overall === 'critical') {
      return Math.max(baseDelay * 8, 3000);  // At least 3 seconds (was 5x)
    }
    if (health.overall === 'degraded') {
      return Math.max(baseDelay * 5, 2000);  // At least 2 seconds (was 3x)
    }
    return Math.max(baseDelay * 2, 1000);    // At least 1 second
  }

  // Reindexing mode - more delay to allow GC
  if (health.overall === 'critical') {
    return Math.max(baseDelay * 5, 1000);    // At least 1 second (was 2x)
  }
  if (health.overall === 'degraded') {
    return Math.max(baseDelay * 3, 500);     // At least 500ms (was 1.5x)
  }
  return Math.max(baseDelay, 100);
}

/**
 * Check if we should pause processing to allow memory recovery
 */
export function shouldPauseForMemory(health: HealthStatus): boolean {
  const memoryPercent = health.server.memoryUsagePercent || 0;
  const heapPercent = health.server.heapUsagePercent || 0;
  return memoryPercent > 88 || heapPercent > 82;
}

/**
 * Get recommended pause duration based on memory pressure
 */
export function getMemoryRecoveryPauseMs(health: HealthStatus): number {
  const memoryPercent = health.server.memoryUsagePercent || 0;

  if (memoryPercent > 95) return 10000; // 10 seconds
  if (memoryPercent > 90) return 5000;  // 5 seconds
  if (memoryPercent > 85) return 2000;  // 2 seconds
  return 0;
}

