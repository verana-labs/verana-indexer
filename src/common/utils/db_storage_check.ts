const CACHE_MS = 60000;
const DEFAULT_DB_STORAGE_MAX_MB = 20 * 1024; // 20 GB
let lastCheck: { sizeMb: number; maxMb: number; overLimit: boolean; at: number } | null = null;


function getDbStorageMaxMb(): number {
  const raw = process.env.DB_STORAGE_MAX_MB;
  if (raw == null || raw === '') return DEFAULT_DB_STORAGE_MAX_MB;
  const n = parseInt(String(raw), 10);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_DB_STORAGE_MAX_MB;
  return n;
}


export async function checkDbStorageLimit(): Promise<{
  overLimit: boolean;
  sizeMb: number;
  maxMb: number;
}> {
  const maxMb = getDbStorageMaxMb();
  if (maxMb <= 0) {
    return { overLimit: false, sizeMb: 0, maxMb: 0 };
  }

  const now = Date.now();
  if (lastCheck && (now - lastCheck.at) < CACHE_MS) {
    return {
      overLimit: lastCheck.overLimit,
      sizeMb: lastCheck.sizeMb,
      maxMb: lastCheck.maxMb,
    };
  }

  try {
    const runRaw = (global as any).__dbStorageRawQuery as undefined | ((sql: string) => Promise<unknown>);
    if (typeof runRaw !== 'function') {
      return { overLimit: false, sizeMb: 0, maxMb };
    }
    const result = await runRaw('SELECT pg_database_size(current_database()) AS size');
    const sizeBytes = Number((result as any)?.rows?.[0]?.size ?? 0);
    const sizeMb = sizeBytes / (1024 * 1024);
    const overLimit = sizeMb >= maxMb;

    lastCheck = { sizeMb, maxMb, overLimit, at: now };
    return { overLimit, sizeMb, maxMb };
  } catch (err) {
    lastCheck = null;
    return { overLimit: false, sizeMb: 0, maxMb };
  }
}
