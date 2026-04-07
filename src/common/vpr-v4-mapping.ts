
const ISSUER_VERIFIER_V3_TO_V4: Record<string, string> = {
  GRANTOR_VALIDATION: "GRANTOR_VALIDATION_PROCESS",
  ECOSYSTEM: "ECOSYSTEM_VALIDATION_PROCESS",
  OPEN: "OPEN",
  MODE_UNSPECIFIED: "MODE_UNSPECIFIED",
  UNKNOWN: "UNKNOWN",
};

const ISSUER_VERIFIER_V4_TO_STORED: Record<string, string> = {
  GRANTOR_VALIDATION_PROCESS: "GRANTOR_VALIDATION",
  ECOSYSTEM_VALIDATION_PROCESS: "ECOSYSTEM",
  OPEN: "OPEN",
  MODE_UNSPECIFIED: "MODE_UNSPECIFIED",
};

export function normalizeIssuerVerifierOnboardingModeV4(mode: string | null | undefined): string {
  if (mode == null || mode === "") return "MODE_UNSPECIFIED";
  const m = String(mode).trim();
  return ISSUER_VERIFIER_V3_TO_V4[m] ?? m;
}

export function normalizeHolderOnboardingModeV4(mode: string | null | undefined): string | null {
  if (mode == null || mode === "") return null;
  const m = String(mode).trim();
  if (m === "ISSUER_VALIDATION" || m === "ISSUER_VALIDATION_PROCESS") return "ISSUER_VALIDATION_PROCESS";
  if (m === "PERMISSIONLESS" || m === "HOLDER_PERMISSIONLESS") return "PERMISSIONLESS";
  return m;
}

export function normalizeIssuerVerifierOnboardingModeForDbFilter(
  mode: string | null | undefined
): string {
  if (mode == null || mode === "") return "";
  const m = String(mode).trim();
  if (m in ISSUER_VERIFIER_V3_TO_V4) return m;
  return ISSUER_VERIFIER_V4_TO_STORED[m] ?? m;
}

export function mapCredentialSchemaApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const issuerSource =
    row.issuer_onboarding_mode != null && String(row.issuer_onboarding_mode).trim() !== ""
      ? String(row.issuer_onboarding_mode)
      : String(row.issuer_perm_management_mode ?? "");
  const verifierSource =
    row.verifier_onboarding_mode != null && String(row.verifier_onboarding_mode).trim() !== ""
      ? String(row.verifier_onboarding_mode)
      : String(row.verifier_perm_management_mode ?? "");
  out.issuer_onboarding_mode = normalizeIssuerVerifierOnboardingModeV4(issuerSource);
  out.verifier_onboarding_mode = normalizeIssuerVerifierOnboardingModeV4(verifierSource);
  const holderRaw = row.holder_onboarding_mode;
  out.holder_onboarding_mode =
    holderRaw != null && holderRaw !== ""
      ? normalizeHolderOnboardingModeV4(String(holderRaw))
      : null;
  return out;
}

export function mapTrustRegistryApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const corp = row.corporation ?? row.controller;
  if (corp != null && corp !== undefined) {
    out.corporation = corp;
    out.controller = row.controller ?? row.corporation;
  }
  return out;
}

export function mapPermissionApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const corp = row.corporation ?? row.grantee;
  if (corp != null && corp !== undefined) {
    out.corporation = corp;
    out.grantee = row.grantee ?? row.corporation;
  }
  return out;
}

export function mapTrustDepositApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const corp = row.corporation ?? row.account;
  if (corp != null && corp !== undefined) {
    out.corporation = corp;
    out.account = row.account ?? row.corporation;
  }
  const dep = row.deposit ?? row.amount;
  if (dep != null && dep !== undefined) {
    out.deposit = dep;
    out.amount = row.amount ?? row.deposit;
  }
  return out;
}
