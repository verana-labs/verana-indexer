
export enum VeranaCredentialSchemaMessageTypes {
  UpdateParams = "/verana.cs.v1.MsgUpdateParams",
  CreateCredentialSchema = "/verana.cs.v1.MsgCreateCredentialSchema",
  CreateCredentialSchemaLegacy = "/veranablockchain.credentialschema.MsgCreateCredentialSchema",
  UpdateCredentialSchema = "/verana.cs.v1.MsgUpdateCredentialSchema",
  ArchiveCredentialSchema = "/verana.cs.v1.MsgArchiveCredentialSchema",
  CreateSchemaAuthorizationPolicyDraft = "/verana.cs.v1.MsgCreateSchemaAuthorizationPolicyDraft",
  ActivateSchemaAuthorizationPolicyVersion = "/verana.cs.v1.MsgActivateSchemaAuthorizationPolicyVersion",
  RevokeSchemaAuthorizationPolicyVersion = "/verana.cs.v1.MsgRevokeSchemaAuthorizationPolicyVersion",
}

export enum VeranaPermissionMessageTypes {
  UpdateParams = "/verana.perm.v1.MsgUpdateParams",
  CreateRootPermission = "/verana.perm.v1.MsgCreateRootPermission",
  CreatePermission = "/verana.perm.v1.MsgCreatePermission",
  StartPermissionVP = "/verana.perm.v1.MsgStartPermissionVP",
  RenewPermissionVP = "/verana.perm.v1.MsgRenewPermissionVP",
  RevokePermission = "/verana.perm.v1.MsgRevokePermission",
  AdjustPermission = "/verana.perm.v1.MsgAdjustPermission",
  SetPermissionVPToValidated = "/verana.perm.v1.MsgSetPermissionVPToValidated",
  CreateOrUpdatePermissionSession = "/verana.perm.v1.MsgCreateOrUpdatePermissionSession",
  SlashPermissionTrustDeposit = "/verana.perm.v1.MsgSlashPermissionTrustDeposit",
  RepayPermissionSlashedTrustDeposit = "/verana.perm.v1.MsgRepayPermissionSlashedTrustDeposit",
  CancelPermissionVPLastRequest = "/verana.perm.v1.MsgCancelPermissionVPLastRequest",
}

export enum VeranaTrustDepositMessageTypes {
  UpdateParams = "/verana.td.v1.MsgUpdateParams",
  AdjustTrustDeposit = "/verana.td.v1.MsgAdjustTrustDeposit",
  ReclaimYield = "/verana.td.v1.MsgReclaimTrustDepositYield",
  ReclaimDeposit = "/verana.td.v1.MsgReclaimTrustDeposit",
  RepaySlashed = "/verana.td.v1.MsgRepaySlashedTrustDeposit",
  SlashTrustDeposit = "/verana.td.v1.MsgSlashTrustDeposit",
  BurnEcosystemSlashedTrustDeposit = "/verana.td.v1.MsgBurnEcosystemSlashedTrustDeposit",
}

export enum VeranaTrustRegistryMessageTypes {
  UpdateParams = "/verana.tr.v1.MsgUpdateParams",
  CreateTrustRegistry = "/verana.tr.v1.MsgCreateTrustRegistry",
  CreateTrustRegistryLegacy = "/veranablockchain.trustregistry.MsgCreateTrustRegistry",
  UpdateTrustRegistry = "/verana.tr.v1.MsgUpdateTrustRegistry",
  ArchiveTrustRegistry = "/verana.tr.v1.MsgArchiveTrustRegistry",
  AddGovernanceFrameworkDoc = "/verana.tr.v1.MsgAddGovernanceFrameworkDocument",
  IncreaseGovernanceFrameworkVersion = "/verana.tr.v1.MsgIncreaseActiveGovernanceFrameworkVersion",
}

export enum UpdateParamsMessageTypes {
  CREDENTIAL_SCHEMA = "/verana.cs.v1.MsgUpdateParams",
  PERMISSION = "/verana.perm.v1.MsgUpdateParams",
  TRUST_DEPOSIT = "/verana.td.v1.MsgUpdateParams",
  TRUST_REGISTRY = "/verana.tr.v1.MsgUpdateParams",
}
export enum CosmosStakingMessageTypes {
  CreateValidator = "/cosmos.staking.v1beta1.MsgCreateValidator",
  EditValidator = "/cosmos.staking.v1beta1.MsgEditValidator",
  Delegate = "/cosmos.staking.v1beta1.MsgDelegate",
  BeginRedelegate = "/cosmos.staking.v1beta1.MsgBeginRedelegate",
  Undelegate = "/cosmos.staking.v1beta1.MsgUndelegate",
  CancelUnbondingDelegation = "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation",
}

export enum CosmosSlashingMessageTypes {
  Unjail = "/cosmos.slashing.v1beta1.MsgUnjail",
}

export const ALL_KNOWN_VERANA_MESSAGE_TYPES = new Set<string>([
  ...Object.values(VeranaCredentialSchemaMessageTypes),
  ...Object.values(VeranaPermissionMessageTypes),
  ...Object.values(VeranaTrustDepositMessageTypes),
  ...Object.values(VeranaTrustRegistryMessageTypes),
  ...Object.values(CosmosStakingMessageTypes),
  ...Object.values(CosmosSlashingMessageTypes),
]);

export function isKnownVeranaMessageType(messageType: string): boolean {
  return ALL_KNOWN_VERANA_MESSAGE_TYPES.has(messageType);
}

export function getAllKnownMessageTypes(): string[] {
  return Array.from(ALL_KNOWN_VERANA_MESSAGE_TYPES);
}

export function shouldSkipUnknownMessages(): boolean {
  return process.env.SKIP_UNKNOWN_MESSAGES === 'true' ||
         process.env.SKIP_UNKNOWN_MESSAGES === '1';
}

export function isVeranaMessageType(messageType: string): boolean {
  return messageType.startsWith('/verana.');
}

export function isTrustRegistryMessageType(messageType: string): boolean {
  return Object.values(VeranaTrustRegistryMessageTypes).includes(messageType as VeranaTrustRegistryMessageTypes);
}

export function isCredentialSchemaMessageType(messageType: string): boolean {
  return Object.values(VeranaCredentialSchemaMessageTypes).includes(messageType as VeranaCredentialSchemaMessageTypes);
}

export function isPermissionMessageType(messageType: string): boolean {
  return Object.values(VeranaPermissionMessageTypes).includes(messageType as VeranaPermissionMessageTypes);
}

export function isTrustDepositMessageType(messageType: string): boolean {
  return Object.values(VeranaTrustDepositMessageTypes).includes(messageType as VeranaTrustDepositMessageTypes);
}

export function isUpdateParamsMessageType(messageType: string): boolean {
  return messageType === VeranaCredentialSchemaMessageTypes.UpdateParams ||
         messageType === VeranaPermissionMessageTypes.UpdateParams ||
         messageType === VeranaTrustDepositMessageTypes.UpdateParams ||
         messageType === VeranaTrustRegistryMessageTypes.UpdateParams;
}