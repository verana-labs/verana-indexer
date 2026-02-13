export enum PermissionType {
  PERMISSION_TYPE_UNSPECIFIED = 0,
  PERMISSION_TYPE_ISSUER = 1,
  PERMISSION_TYPE_VERIFIER = 2,
  PERMISSION_TYPE_ISSUER_GRANTOR = 3,
  PERMISSION_TYPE_VERIFIER_GRANTOR = 4,
  /** PERMISSION_TYPE_ECOSYSTEM - Changed from PERMISSION_TYPE_TRUST_REGISTRY */
  PERMISSION_TYPE_ECOSYSTEM = 5,
  PERMISSION_TYPE_HOLDER = 6,
  UNRECOGNIZED = -1,
}
export enum PermissionTypeNames {
  UNSPECIFIED = 0,
  ISSUER = 1,
  VERIFIER = 2,
  ISSUER_GRANTOR = 3,
  VERIFIER_GRANTOR = 4,
  ECOSYSTEM = 5,
  HOLDER = 6,
}
export function getPermissionTypeString(msg: MsgStartPermissionVP): string {
  if (typeof msg.type === 'string') {
    return msg.type;
  }
  if (typeof msg.type === 'number') {
    return PermissionTypeNames[msg.type] ?? "UNKNOWN";
  }
  return "UNKNOWN";
}
export interface MsgCreateRootPermission {
  creator: string;
  schema_id: number;
  did: string;
  country: string;
  effective_from?: Date | undefined;
  timestamp?: Date | undefined;
  effective_until?: Date | undefined;
  validation_fees: number;
  issuance_fees: number;
  verification_fees: number;
}
export interface MsgStartPermissionVP {
  creator: string;
  timestamp?: Date | undefined;
  type: number;
  validator_perm_id: number;
  country: string;
  did: string;
  effective_from: Date | undefined;
  effective_until: Date | undefined;
}
export interface MsgSlashPermissionTrustDeposit {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
  amount: number;
}
export interface MsgSetPermissionVPToValidated {
  creator: string;
  id: number;
  effective_until?: Date | undefined;
  timestamp?: Date | undefined;
  validation_fees: number;
  issuance_fees: number;
  verification_fees: number;
  country: string;
  vp_summary_digest_sri: string;
}

export interface MsgRevokePermission {
  timestamp: Date | string;
  id: number;
  creator: string;
}
export interface MsgRepayPermissionSlashedTrustDeposit {
  timestamp: Date | string;
  creator: string;
  id: number;
}
export interface MsgRenewPermissionVP {
  creator: string;
  timestamp: Date | string;
  id: number;
}
export interface MsgExtendPermission {
  creator: string;
  id: number;
  effective_until?: Date | undefined;
  timestamp?: Date | undefined;
}
export interface MsgCreatePermission {
  creator: string;
  schema_id: number;
  type: PermissionType;
  did: string;
  country: string;
  effective_from?: Date | undefined;
  timestamp?: Date | undefined;
  effective_until?: Date | undefined;
  verification_fees: number;
}
export interface MsgCreateOrUpdatePermissionSession {
  timestamp?: Date | undefined;
  creator: string;
  id: string;
  issuerPermId?: number;
  issuer_perm_id?: number;
  verifierPermId?: number;
  verifier_perm_id?: number;
  agentPermId?: number;
  agent_perm_id?: number;
  walletAgentPermId?: number;
  wallet_agent_perm_id?: number;
}
export interface MsgCancelPermissionVPLastRequest {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
}
export interface MsgConfirmPermissionVPTermination {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
}
export interface MsgRequestPermissionVPTermination {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
}
