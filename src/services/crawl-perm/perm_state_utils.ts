export type PermState = "REPAID" | "SLASHED" | "REVOKED" | "EXPIRED" | "ACTIVE" | "FUTURE" | "INACTIVE";

export type CorporationAction = "VP_RENEW" | "VP_CANCEL" | "PERM_REVOKE" | "PERM_ADJUST" | "PERM_REPAY";
export type GranteeAction = CorporationAction;
export type ValidatorAction = "VP_SET_VALIDATED" | "PERM_REVOKE" | "PERM_ADJUST" | "PERM_SLASH";

export type PermissionType = "UNSPECIFIED" | "ISSUER_GRANTOR" | "ISSUER" | "VERIFIER_GRANTOR" | "VERIFIER" | "HOLDER" | "ECOSYSTEM";
export type ValidationState = "VALIDATION_STATE_UNSPECIFIED" | "PENDING" | "VALIDATED" | null;
export type SchemaMode = "GRANTOR_VALIDATION" | "OPEN" | "ECOSYSTEM";

export interface PermissionData {
  repaid?: string | null;
  slashed?: string | null;
  revoked?: string | null;
  effective_from?: string | null;
  effective_until?: string | null;
  type: PermissionType;
  vp_state?: ValidationState;
  vp_exp?: string | null;
  validator_perm_id?: string | null;
}

export interface SchemaData {
  issuer_onboarding_mode?: string;
  verifier_onboarding_mode?: string;
}

export const PENDING_FLAT_VP_PENDING_PERM_STATES: ReadonlySet<PermState> = new Set([
  "INACTIVE",
  "ACTIVE",
  "FUTURE",
  "EXPIRED",
]);

export function pendingFlatMatchesVpPendingWithEligiblePermState(perm: {
  vp_state?: string | null;
  perm_state?: string | null;
}): boolean {
  if (String(perm.vp_state ?? "").toUpperCase() !== "PENDING") return false;
  const ps = perm.perm_state as PermState | undefined;
  return ps !== undefined && PENDING_FLAT_VP_PENDING_PERM_STATES.has(ps);
}

export const PENDING_FLAT_VALIDATOR_PARENT_TYPES: ReadonlySet<string> = new Set([
  "ISSUER_GRANTOR",
  "VERIFIER_GRANTOR",
  "ECOSYSTEM",
  "ISSUER",
]);

export function calculatePermState(perm: PermissionData, now: Date = new Date()): PermState {
  if (perm.repaid !== null && perm.repaid !== undefined) {
    return "REPAID";
  }
  
  if (perm.slashed !== null && perm.slashed !== undefined) {
    return "SLASHED";
  }
  
  if (perm.revoked !== null && perm.revoked !== undefined) {
    const revokedDate = new Date(perm.revoked);
    if (!Number.isNaN(revokedDate.getTime()) && revokedDate < now) {
      return "REVOKED";
    }
  }
  
  if (perm.effective_until !== null && perm.effective_until !== undefined) {
    const untilDate = new Date(perm.effective_until);
    if (!Number.isNaN(untilDate.getTime()) && untilDate < now) {
      return "EXPIRED";
    }
  }
  
  if (perm.effective_from !== null && perm.effective_from !== undefined) {
    const fromDate = new Date(perm.effective_from);
    if (!Number.isNaN(fromDate.getTime())) {
      if (fromDate <= now) {
        return "ACTIVE";
      }
      return "FUTURE";
    }
  }
  
  return "INACTIVE";
}

function normalizeSchemaMode(mode?: string): SchemaMode {
  if (!mode) return "OPEN";
  const upper = mode.toUpperCase();
  if (
    upper === "GRANTOR_VALIDATION"
    || upper === "GRANTOR"
    || upper === "GRANTOR_VALIDATION_PROCESS"
  ) {
    return "GRANTOR_VALIDATION";
  }
  if (upper === "OPEN") return "OPEN";
  if (upper === "ECOSYSTEM" || upper === "ECOSYSTEM_VALIDATION_PROCESS") return "ECOSYSTEM";
  return "OPEN";
}

function normalizePermissionType(value: unknown): PermissionType {
  if (typeof value === "string") {
    const upper = value.toUpperCase();
    if (
      upper === "UNSPECIFIED" ||
      upper === "ISSUER" || upper === "VERIFIER" || upper === "ISSUER_GRANTOR" ||
      upper === "VERIFIER_GRANTOR" || upper === "ECOSYSTEM" || upper === "HOLDER"
    ) {
      return upper as PermissionType;
    }
  }
  const n = Number(value);
  switch (n) {
    case 0: return "UNSPECIFIED";
    case 1: return "ISSUER";
    case 2: return "VERIFIER";
    case 3: return "ISSUER_GRANTOR";
    case 4: return "VERIFIER_GRANTOR";
    case 5: return "ECOSYSTEM";
    case 6: return "HOLDER";
    default: return "UNSPECIFIED";
  }
}

function normalizeVpState(value: unknown): ValidationState {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const upper = value.toUpperCase();
    if (upper === "VALIDATION_STATE_UNSPECIFIED" || upper === "UNSPECIFIED") return "VALIDATION_STATE_UNSPECIFIED";
    if (upper === "PENDING") return "PENDING";
    if (upper === "VALIDATED") return "VALIDATED";
    if (upper === "TERMINATED" || upper === "TERMINATION_REQUESTED") return "VALIDATED";
    return null;
  }
  const n = Number(value);
  if (n === 1) return "PENDING";
  if (n === 2) return "VALIDATED";
  if (n === 3 || n === 4) return "VALIDATED";
  return "VALIDATION_STATE_UNSPECIFIED";
}

function isIssuerType(type: PermissionType): boolean {
  return type === "ISSUER_GRANTOR" || type === "ISSUER";
}

function isVerifierType(type: PermissionType): boolean {
  return type === "VERIFIER_GRANTOR" || type === "VERIFIER";
}

export function calculateCorporationAvailableActions(
  perm: PermissionData,
  schema: SchemaData,
  validatorPermState?: PermState | null,
  now: Date = new Date()
): CorporationAction[] {
  const actions: Set<CorporationAction> = new Set();
  const type = normalizePermissionType(perm.type);
  const vpState = normalizeVpState(perm.vp_state);
  const permState = calculatePermState(perm, now);
  const issuerMode = normalizeSchemaMode(schema.issuer_onboarding_mode);
  const verifierMode = normalizeSchemaMode(schema.verifier_onboarding_mode);
  const vpExp = perm.vp_exp ? new Date(perm.vp_exp) : null;
  const isVpExpired = vpExp !== null && !Number.isNaN(vpExp.getTime()) && vpExp < now;

  const isValidatorActive = validatorPermState === "ACTIVE";
  if (isIssuerType(type)) {
    if (issuerMode === "GRANTOR_VALIDATION" || issuerMode === "ECOSYSTEM") {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "VALIDATED" && !isVpExpired) {
          if (isValidatorActive) {
            actions.add("VP_RENEW");
          }
          if (permState === "ACTIVE" || permState === "FUTURE") {
            actions.add("PERM_REVOKE");
          }
        } else if (vpState === "PENDING") {
          actions.add("VP_CANCEL");
          if (permState === "ACTIVE" || permState === "FUTURE") {
            actions.add("PERM_REVOKE");
          }
        }
      }
    } else if (issuerMode === "OPEN") {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_ADJUST");
      }
    }
  }

  if (isVerifierType(type)) {
    const inVpFlow = vpState !== null && vpState !== "VALIDATION_STATE_UNSPECIFIED";
    const useVpFlowRules = verifierMode === "GRANTOR_VALIDATION" || verifierMode === "ECOSYSTEM" || inVpFlow;
    if (useVpFlowRules) {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "VALIDATED" && !isVpExpired) {
          if (isValidatorActive) {
            actions.add("VP_RENEW");
          }
          if (permState === "ACTIVE" || permState === "FUTURE") {
            actions.add("PERM_REVOKE");
          }
        } else if (vpState === "PENDING") {
          actions.add("VP_CANCEL");
          if (permState === "ACTIVE" || permState === "FUTURE") {
            actions.add("PERM_REVOKE");
          }
        }
      }
    } else if (verifierMode === "OPEN" && !inVpFlow) {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_ADJUST");
      }
    }
  }

  if (type === "HOLDER") {
    if (permState === "REPAID" || permState === "REVOKED") {
    } else if (permState === "SLASHED") {
      actions.add("PERM_REPAY");
    } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
      if (vpState === "VALIDATED" && !isVpExpired) {
        if (isValidatorActive) {
          actions.add("VP_RENEW");
        }
        if (permState === "ACTIVE" || permState === "FUTURE") {
          actions.add("PERM_REVOKE");
        }
      } else if (vpState === "PENDING") {
        actions.add("VP_CANCEL");
        if (permState === "ACTIVE" || permState === "FUTURE") {
          actions.add("PERM_REVOKE");
        }
      }
    }
  }

  if (type === "ECOSYSTEM") {
    if (permState === "REPAID" || permState === "REVOKED") {
    } else if (permState === "SLASHED") {
      actions.add("PERM_REPAY");
    } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
      actions.add("PERM_REVOKE");
      actions.add("PERM_ADJUST");
    }
  }

  return Array.from(actions).sort();
}

export const calculateGranteeAvailableActions = calculateCorporationAvailableActions;

export function calculateValidatorAvailableActions(
  perm: PermissionData,
  schema: SchemaData,
  now: Date = new Date()
): ValidatorAction[] {
  const actions: Set<ValidatorAction> = new Set();
  const type = normalizePermissionType(perm.type);
  const vpState = normalizeVpState(perm.vp_state);
  const permState = calculatePermState(perm, now);
  const issuerMode = normalizeSchemaMode(schema.issuer_onboarding_mode);
  const verifierMode = normalizeSchemaMode(schema.verifier_onboarding_mode);
  const vpExp = perm.vp_exp ? new Date(perm.vp_exp) : null;
  if (isIssuerType(type)) {
    if (issuerMode === "GRANTOR_VALIDATION" || issuerMode === "ECOSYSTEM") {
      actions.add("PERM_SLASH");
      
      if (permState === "ACTIVE" || permState === "FUTURE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_ADJUST");
      }
      
      if (permState === "ACTIVE" || permState === "FUTURE") {
        if (vpState === "VALIDATED" && vpExp && !Number.isNaN(vpExp.getTime())) {
        }
      }
      
      if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "PENDING") {
          actions.add("VP_SET_VALIDATED");
        }
      }
    }
  }

  if (isVerifierType(type)) {
    const inVpFlow = vpState !== null && vpState !== "VALIDATION_STATE_UNSPECIFIED";
    const useVpFlowRules = verifierMode === "GRANTOR_VALIDATION" || verifierMode === "ECOSYSTEM" || inVpFlow;
    if (useVpFlowRules) {
      actions.add("PERM_SLASH");

      if (permState === "ACTIVE" || permState === "FUTURE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_ADJUST");
      }

      if (permState === "ACTIVE" || permState === "FUTURE") {
        if (vpState === "VALIDATED" && vpExp && !Number.isNaN(vpExp.getTime())) {
        }
      }

      if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "PENDING") {
          actions.add("VP_SET_VALIDATED");
        }
      }
    }
  }

  if (type === "HOLDER") {
    actions.add("PERM_SLASH");
    
    if (permState === "ACTIVE" || permState === "FUTURE") {
      actions.add("PERM_REVOKE");
      actions.add("PERM_ADJUST");
    }
    
    if (permState === "ACTIVE" || permState === "FUTURE") {
      if (vpState === "VALIDATED" && vpExp && !Number.isNaN(vpExp.getTime())) {
      }
    }
    
    if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
      if (vpState === "PENDING") {
        actions.add("VP_SET_VALIDATED");
      }
    }
  }

  return Array.from(actions).sort();
}

