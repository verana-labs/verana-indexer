export type PermState = "REPAID" | "SLASHED" | "REVOKED" | "EXPIRED" | "ACTIVE" | "FUTURE" | "INACTIVE";

export type GranteeAction = "VP_RENEW" | "VP_CANCEL" | "PERM_REVOKE" | "PERM_EXTEND" | "PERM_REPAY";
export type ValidatorAction = "VP_SET_VALIDATED" | "PERM_REVOKE" | "PERM_EXTEND" | "PERM_SLASH";

export type PermissionType = "ISSUER_GRANTOR" | "ISSUER" | "VERIFIER_GRANTOR" | "VERIFIER" | "HOLDER" | "ECOSYSTEM";
export type ValidationState = "VALIDATION_STATE_UNSPECIFIED" | "PENDING" | "VALIDATED" | "TERMINATED" | null;
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
  issuer_perm_management_mode?: string;
  verifier_perm_management_mode?: string;
}

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
  if (upper === "GRANTOR_VALIDATION" || upper === "GRANTOR") return "GRANTOR_VALIDATION";
  if (upper === "OPEN") return "OPEN";
  if (upper === "ECOSYSTEM") return "ECOSYSTEM";
  return "OPEN";
}

function isIssuerType(type: PermissionType): boolean {
  return type === "ISSUER_GRANTOR" || type === "ISSUER";
}

function isVerifierType(type: PermissionType): boolean {
  return type === "VERIFIER_GRANTOR" || type === "VERIFIER";
}

export function calculateGranteeAvailableActions(
  perm: PermissionData,
  schema: SchemaData,
  validatorPermState?: PermState | null,
  now: Date = new Date()
): GranteeAction[] {
  const actions: Set<GranteeAction> = new Set();
  const permState = calculatePermState(perm, now);
  const issuerMode = normalizeSchemaMode(schema.issuer_perm_management_mode);
  const verifierMode = normalizeSchemaMode(schema.verifier_perm_management_mode);
  const vpState = perm.vp_state || null;
  const vpExp = perm.vp_exp ? new Date(perm.vp_exp) : null;
  const isVpExpired = vpExp !== null && !Number.isNaN(vpExp.getTime()) && vpExp < now;

  const isValidatorActive = validatorPermState === "ACTIVE";
  if (isIssuerType(perm.type)) {
    if (issuerMode === "GRANTOR_VALIDATION" || issuerMode === "ECOSYSTEM") {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "TERMINATED") {
        } else if (vpState === "VALIDATED" && !isVpExpired) {
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
        actions.add("PERM_EXTEND");
      }
    }
  }

  if (isVerifierType(perm.type)) {
    if (verifierMode === "GRANTOR_VALIDATION" || verifierMode === "ECOSYSTEM") {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        if (vpState === "TERMINATED") {
        } else if (vpState === "VALIDATED" && !isVpExpired) {
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
    } else if (verifierMode === "OPEN") {
      if (permState === "REPAID" || permState === "REVOKED") {
      } else if (permState === "SLASHED") {
        actions.add("PERM_REPAY");
      } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_EXTEND");
      }
    }
  }

  if (perm.type === "HOLDER") {
    if (permState === "REPAID" || permState === "REVOKED") {
    } else if (permState === "SLASHED") {
      actions.add("PERM_REPAY");
    } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
      if (vpState === "TERMINATED") {
      } else if (vpState === "VALIDATED" && !isVpExpired) {
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

  if (perm.type === "ECOSYSTEM") {
    if (permState === "REPAID" || permState === "REVOKED") {
    } else if (permState === "SLASHED") {
      actions.add("PERM_REPAY");
    } else if (permState === "ACTIVE" || permState === "FUTURE" || permState === "INACTIVE") {
      actions.add("PERM_REVOKE");
      actions.add("PERM_EXTEND");
    }
  }

  return Array.from(actions).sort();
}

export function calculateValidatorAvailableActions(
  perm: PermissionData,
  schema: SchemaData,
  now: Date = new Date()
): ValidatorAction[] {
  const actions: Set<ValidatorAction> = new Set();
  const permState = calculatePermState(perm, now);
  const issuerMode = normalizeSchemaMode(schema.issuer_perm_management_mode);
  const verifierMode = normalizeSchemaMode(schema.verifier_perm_management_mode);
  const vpState = perm.vp_state || null;
  const vpExp = perm.vp_exp ? new Date(perm.vp_exp) : null;
  if (isIssuerType(perm.type)) {
    if (issuerMode === "GRANTOR_VALIDATION" || issuerMode === "ECOSYSTEM") {
      actions.add("PERM_SLASH");
      
      if (permState === "ACTIVE" || permState === "FUTURE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_EXTEND");
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

  if (isVerifierType(perm.type)) {
    if (verifierMode === "GRANTOR_VALIDATION" || verifierMode === "ECOSYSTEM") {
      actions.add("PERM_SLASH");
      
      if (permState === "ACTIVE" || permState === "FUTURE") {
        actions.add("PERM_REVOKE");
        actions.add("PERM_EXTEND");
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

  if (perm.type === "HOLDER") {
    actions.add("PERM_SLASH");
    
    if (permState === "ACTIVE" || permState === "FUTURE") {
      actions.add("PERM_REVOKE");
      actions.add("PERM_EXTEND");
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

