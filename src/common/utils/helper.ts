export function sleep(ms: number) {
  // eslint-disable-next-line no-promise-executor-return
  return new Promise((r) => setTimeout(r, ms))
}

let canonicalizeLoader: Promise<(v: unknown) => string> | null = null

export async function canonicalizeJson(value: unknown): Promise<string> {
  if (!canonicalizeLoader) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    canonicalizeLoader = import('canonicalize').then((mod: any) => mod?.default ?? mod)
  }
  const fn = await canonicalizeLoader
  const out = fn(value)
  if (typeof out !== 'string') throw new Error('canonicalize did not return a string')
  return out
}

export function toCoin(value: unknown, denom = 'uvna'): string {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return `0${denom}`
  return `${Math.trunc(n)}${denom}`
}

export function toJsonbColumn(value: unknown): string | null {
  return value === null || value === undefined ? null : JSON.stringify(value)
}
