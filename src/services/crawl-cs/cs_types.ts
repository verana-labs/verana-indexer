export function getModeString(modeNumber: number): string {
    const modes: Record<number, string> = {
        0: "MODE_UNSPECIFIED",
        1: "OPEN",
        2: "GRANTOR_VALIDATION",
        3: "ECOSYSTEM",
    };

    return modes[modeNumber] || "UNKNOWN";
}

export function getHolderOnboardingModeString(modeNumber: number): string {
  const modes: Record<number, string> = {
    0: "MODE_UNSPECIFIED",
    1: "ISSUER_VALIDATION_PROCESS",
    2: "PERMISSIONLESS",
  };
  return modes[modeNumber] ?? "UNKNOWN";
}