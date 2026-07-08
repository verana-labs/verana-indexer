export function getModeString(modeNumber: number): string {
  const modes: Record<number, string> = {
    0: 'MODE_UNSPECIFIED',
    1: 'OPEN',
    2: 'ECOSYSTEM_ONBOARDING_PROCESS',
    3: 'GRANTOR_ONBOARDING_PROCESS',
  }

  return modes[modeNumber] ?? 'MODE_UNSPECIFIED'
}

export function getHolderOnboardingModeString(modeNumber: number): string {
  const modes: Record<number, string> = {
    0: 'MODE_UNSPECIFIED',
    1: 'ISSUER_ONBOARDING_PROCESS',
    2: 'PERMISSIONLESS',
  }
  return modes[modeNumber] ?? 'MODE_UNSPECIFIED'
}
