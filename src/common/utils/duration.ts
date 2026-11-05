import type { Duration } from '@verana-labs/verana-types/codec/google/protobuf/duration'

// Protobuf JSON duration: integer seconds, at most nine fractional digits, no float rounding.
export function serializeDuration(duration: Duration | undefined): string | null {
  if (!duration) return null
  const seconds = Number(duration.seconds ?? 0)
  const nanos = Number(duration.nanos ?? 0)
  if (seconds === 0 && nanos === 0) return null
  const fraction = nanos ? `.${String(nanos).padStart(9, '0').replace(/0+$/, '')}` : ''
  return `${seconds}${fraction}s`
}
