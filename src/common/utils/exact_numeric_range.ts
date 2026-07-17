const INTEGER_PATTERN = /^[+-]?\d+$/

export const INTEGER_PARAM_PATTERN = /^\d+$/

export function parseExactInteger(value: unknown): bigint | undefined {
  if (value === undefined || value === null || value === '') return undefined
  if (typeof value === 'bigint') return value
  if (typeof value === 'number') {
    return Number.isFinite(value) ? BigInt(Math.trunc(value)) : undefined
  }
  const raw = String(value).trim()
  return INTEGER_PATTERN.test(raw) ? BigInt(raw) : undefined
}

export function isImpossibleExactRange(min: unknown, max: unknown): boolean {
  const minValue = parseExactInteger(min)
  const maxValue = parseExactInteger(max)
  return minValue !== undefined && maxValue !== undefined && minValue >= maxValue
}

export function applyExactRangeToQuery(query: any, column: string, min?: string | number, max?: string | number): any {
  const minValue = parseExactInteger(min)
  const maxValue = parseExactInteger(max)

  if (minValue !== undefined && maxValue !== undefined && minValue >= maxValue) {
    return query.whereRaw('1 = 0')
  }

  let nextQuery = query
  if (minValue !== undefined) {
    nextQuery = nextQuery.whereRaw('?? >= ?::numeric', [column, minValue.toString()])
  }
  if (maxValue !== undefined) {
    nextQuery = nextQuery.whereRaw('?? < ?::numeric', [column, maxValue.toString()])
  }
  return nextQuery
}

export function filterRowsByExactRange<T>(
  rows: T[],
  min: string | number | undefined,
  max: string | number | undefined,
  readValue: (row: T) => unknown
): T[] {
  const minValue = parseExactInteger(min)
  const maxValue = parseExactInteger(max)

  if (minValue === undefined && maxValue === undefined) return rows
  if (minValue !== undefined && maxValue !== undefined && minValue >= maxValue) return []

  return rows.filter((row) => {
    const value = parseExactInteger(readValue(row)) ?? BigInt(0)
    if (minValue !== undefined && value < minValue) return false
    if (maxValue !== undefined && value >= maxValue) return false
    return true
  })
}
