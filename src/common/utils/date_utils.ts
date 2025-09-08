export function formatTimestamp(rawTimestamp: any): string {
    let date: Date;

    if (typeof rawTimestamp === "string") {
        // Remove trailing timezone text like " +0000 UTC"
        const cleaned = rawTimestamp.replace(/ \+\d{4} UTC$/, '');
        date = new Date(cleaned);
    } else if (typeof rawTimestamp === "number") {
        // Assume timestamp in milliseconds
        date = new Date(rawTimestamp);
    } else if (rawTimestamp instanceof Date) {
        date = rawTimestamp;
    } else {
        // Unknown type, fallback
        return String(rawTimestamp);
    }

    if (Number.isNaN(date.getTime())) {
        return String(rawTimestamp);
    }

    return date.toISOString();
}


export function addYearsToDate(dateStr: string | undefined, years: string | number): any {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    const yearsToAdd = typeof years === 'string' ? parseInt(years, 10) : years;

    if (Number.isNaN(yearsToAdd)) {
        throw new Error(`Invalid number of years: ${years}`);
    }

    date.setFullYear(date.getFullYear() + yearsToAdd);
    return date.toISOString();
}

