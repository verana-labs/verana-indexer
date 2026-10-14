export function toDate(value: unknown): Date | null {
  if (value == null) return null
  const d = value instanceof Date ? value : new Date(value as string)
  return Number.isFinite(d.getTime()) ? d : null
}

export function toIso(value: unknown): string | undefined {
  const d = toDate(value)
  return d ? d.toISOString() : undefined
}

export function formatTimestamp(rawTimestamp: any): string {
  return toIso(rawTimestamp) ?? String(rawTimestamp)
}

export function addYearsToDate(dateStr: string | undefined, years: string | number): any {
  if (!dateStr) return null
  const date = new Date(dateStr)
  const yearsToAdd = typeof years === 'string' ? parseInt(years, 10) : years

  if (Number.isNaN(yearsToAdd)) {
    throw new Error(`Invalid number of years: ${years}`)
  }

  date.setFullYear(date.getFullYear() + yearsToAdd)
  return date.toISOString()
}

const UTC_DATETIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/

function withoutFractionalSeconds(timestamp: string): string {
  return timestamp.replace(/\.\d+Z$/, 'Z')
}

export function parseDatetimeParam(
  value: unknown,
  paramName: string
): { ok: true; value: string } | { ok: false; message: string } {
  const rejection = {
    ok: false as const,
    message: `"${paramName}" must be an ISO 8601 UTC datetime with a trailing Z (e.g. 2026-01-18T10:00:00Z)`,
  }

  if (typeof value !== 'string' || !UTC_DATETIME_PATTERN.test(value)) {
    return rejection
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime()) || date.getUTCFullYear() === 0) {
    return rejection
  }
  if (withoutFractionalSeconds(date.toISOString()) !== withoutFractionalSeconds(value)) {
    return rejection
  }

  return { ok: true, value }
}

/**
 * Normalizes a date-like value to an ISO 8601 string, or null.
 * Accepts `Date` instances and strings.
 */
export function dateToIsoOrNull(value: unknown): string | null {
  if (!value) return null
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString()
  }
  return typeof value === 'string' ? value : null
}
