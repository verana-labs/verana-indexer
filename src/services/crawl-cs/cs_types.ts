export function getModeString(modeNumber: number): string {
    const modes: Record<number, string> = {
        0: "MODE_UNSPECIFIED",
        1: "OPEN",
        2: "GRANTOR_VALIDATION",
        3: "ECOSYSTEM",
    };

    return modes[modeNumber] || "UNKNOWN";
}