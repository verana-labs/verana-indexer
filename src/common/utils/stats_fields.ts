

export const STATS_FIELDS_COMMON: readonly string[] = [
    "participants",
    "participants_ecosystem",
    "participants_issuer_grantor",
    "participants_issuer",
    "participants_verifier_grantor",
    "participants_verifier",
    "participants_holder",
    "weight",
    "issued",
    "verified",
    "ecosystem_slash_events",
    "ecosystem_slashed_amount",
    "ecosystem_slashed_amount_repaid",
    "network_slash_events",
    "network_slashed_amount",
    "network_slashed_amount_repaid",
];

export const TR_STATS_FIELDS: readonly string[] = [
    ...STATS_FIELDS_COMMON.slice(0, 7), 
    "active_schemas",
    "archived_schemas",
    ...STATS_FIELDS_COMMON.slice(7),
];

export const CS_STATS_FIELDS: readonly string[] = [...STATS_FIELDS_COMMON];

export function statsToUpdateObject(
    stats: Record<string, unknown> | null | undefined,
    fields: readonly string[]
): Record<string, number> {
    const out: Record<string, number> = {};
    for (const field of fields) {
        out[field] = Number((stats as any)?.[field] ?? 0);
    }
    return out;
}

export function getDefaultStatsObject(fields: readonly string[]): Record<string, number> {
    const out: Record<string, number> = {};
    for (const field of fields) {
        out[field] = 0;
    }
    return out;
}

export function applyStatsToTarget(
    target: Record<string, any>,
    stats: Record<string, unknown> | null | undefined,
    fields: readonly string[]
): void {
    const out = target;
    for (const field of fields) {
        const v = (stats as any)?.[field];
        out[field] = typeof v === "number" && Number.isFinite(v) ? v : Number(v ?? 0);
    }
}
