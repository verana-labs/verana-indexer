
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
  delete out.issuer_perm_management_mode;
  delete out.verifier_perm_management_mode;
  return out;
}

export function mapTrustRegistryApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const corp = out.corporation ?? out.controller;
  out.corporation = corp ?? null;
  delete out.controller;
  return out;
}

export function normalizeVprFeeDiscountRatio(v: unknown): number {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  if (n > 1) return Math.min(1, n / 10000);
  return Math.min(1, Math.max(0, n));
}

export function mergePermissionExtendedAdjustedAliases(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const ts = out.adjusted ?? out.extended;
  const by = out.adjusted_by ?? out.extended_by;
  out.adjusted = ts ?? null;
  out.adjusted_by = by ?? null;
  delete out.extended;
  delete out.extended_by;
  return out;
}

export function mapPermissionApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const merged = mergePermissionExtendedAdjustedAliases({ ...row });
  const out: Record<string, unknown> = { ...merged };
  const corp = out.corporation ?? out.grantee ?? out.authority;
  out.corporation = corp ?? null;
  delete out.grantee;
  delete out.authority;
  if (out.vp_summary_digest == null && out.vp_summary_digest_sri != null) {
    out.vp_summary_digest = out.vp_summary_digest_sri;
  }
  delete out.vp_summary_digest_sri;
  if ("issuance_fee_discount" in out) {
    out.issuance_fee_discount = normalizeVprFeeDiscountRatio(out.issuance_fee_discount);
  }
  if ("verification_fee_discount" in out) {
    out.verification_fee_discount = normalizeVprFeeDiscountRatio(out.verification_fee_discount);
  }
  delete out.created_by;
  delete out.revoked_by;
  delete out.slashed_by;
  delete out.repaid_by;
  delete out.country;
  delete out.vp_term_requested;
  return out;
}

export function mapTrustDepositApiFields(
  row: Record<string, unknown>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  const corp = out.corporation ?? out.account;
  if (corp !== undefined) {
    out.corporation = corp;
  }
  delete out.account;
  const dep = out.deposit ?? out.amount;
  if (dep != null && dep !== undefined) {
    out.deposit = dep;
  }
  delete out.amount;
  delete out.last_repaid_by;
  return out;
}
