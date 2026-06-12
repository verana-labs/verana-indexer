export function toDate(value: unknown): Date | null {
  if (value == null) return null;
  const d = value instanceof Date ? value : new Date(value as string);
  return Number.isFinite(d.getTime()) ? d : null;
}

export function toIso(value: unknown): string | undefined {
  const d = toDate(value);
  return d ? d.toISOString() : undefined;
}

export function formatTimestamp(rawTimestamp: any): string {
    const d = toDate(rawTimestamp);
    if (d) return d.toISOString();
    return String(rawTimestamp);
}


export function addYearsToDate(dateStr: string | undefined, years: number | number): any {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    const yearsToAdd = typeof years === 'string' ? parseInt(years, 10) : years;

    if (Number.isNaN(yearsToAdd)) {
        throw new Error(`Invalid number of years: ${years}`);
    }

    date.setFullYear(date.getFullYear() + yearsToAdd);
    return date.toISOString();
}


export function isValidISO8601UTC(timestamp: string): boolean {
    if (typeof timestamp !== 'string' || timestamp.trim().length === 0) {
        return false;
    }

    const iso8601Pattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;

    if (!iso8601Pattern.test(timestamp)) {
        return false;
    }

    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
        return false;
    }

    const reconstructed = date.toISOString();
    const normalizedInput = timestamp.replace(/\.\d{3}Z$/, 'Z');
    const normalizedReconstructed = reconstructed.replace(/\.\d{3}Z$/, 'Z');
    return normalizedInput === normalizedReconstructed;
}

