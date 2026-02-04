import Redis from 'ioredis';

let cacheCleared = false;

export interface CacheCleanResult {
  success: boolean;
  message: string;
  keysCleared?: number;
}

export async function clearRedisCache(logger?: any): Promise<CacheCleanResult> {
  if (cacheCleared) {
    return { success: true, message: 'Cache already cleared in this session' };
  }

  const redisUrl = process.env.CACHER || process.env.QUEUE_JOB_REDIS;
  
  if (!redisUrl) {
    const msg = 'No Redis URL configured';
    logger?.warn?.(msg);
    return { success: false, message: msg };
  }

  let redis: Redis | null = null;
  
  try {
    const url = new URL(redisUrl.replace('redis://', 'http://'));
    const dbString = url.pathname ? url.pathname.substr(1) : '';
    const db = dbString && !Number.isNaN(parseInt(dbString, 10)) ? parseInt(dbString, 10) : 0;
    
    redis = new Redis({
      host: url.hostname,
      port: parseInt(url.port, 10) || 6379,
      password: url.password || undefined,
      db,
      maxRetriesPerRequest: 3,
      retryStrategy: (times) => {
        if (times > 3) return null;
        return Math.min(times * 100, 1000);
      },
    });

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Redis connection timeout')), 5000);
      redis!.on('ready', () => {
        clearTimeout(timeout);
        resolve();
      });
      redis!.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    logger?.info?.('Clearing Redis cache for reindex');

    const prefixes = ['MOL-', 'bull:', 'verana:', 'cache:', 'tr:', 'cs:', 'did:', 'perm:'];
    let totalCleared = 0;

    for (const prefix of prefixes) {
      try {
        const keys = await redis.keys(`${prefix}*`);
        if (keys.length > 0) {
          await redis.del(...keys);
          totalCleared += keys.length;
          logger?.info?.(`Cleared ${keys.length} keys with prefix ${prefix}`);
        }
      } catch (err: any) {
        logger?.warn?.(`Failed to clear keys with prefix ${prefix}: ${err.message}`);
      }
    }

    try {
      await redis.flushdb();
      logger?.info?.('Flushed current Redis database');
    } catch (err: any) {
      logger?.warn?.(`Could not flush database: ${err.message}`);
    }

    cacheCleared = true;
    const msg = `Redis cache cleared: ${totalCleared} keys removed`;
    logger?.info?.(msg);
    
    return { success: true, message: msg, keysCleared: totalCleared };
  } catch (error: any) {
    const msg = `Failed to clear Redis cache: ${error.message}`;
    logger?.error?.(msg);
    return { success: false, message: msg };
  } finally {
    if (redis) {
      try {
        await redis.quit();
      } catch {
      }
    }
  }
}

export function resetCacheClearFlag(): void {
  cacheCleared = false;
}
