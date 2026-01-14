
export enum VeranaDidMessageTypes {
  UpdateParams = "/verana.dd.v1.MsgUpdateParams",
  AddDid = "/verana.dd.v1.MsgAddDID",
  AddDidLegacy = "/veranablockchain.diddirectory.MsgAddDID",
  RenewDid = "/verana.dd.v1.MsgRenewDID",
  RenewDidLegacy = "/veranablockchain.diddirectory.MsgRenewDID",
  RemoveDid = "/verana.dd.v1.MsgRemoveDID",
  RemoveDidLegacy = "/veranablockchain.diddirectory.MsgRemoveDID",
  TouchDid = "/verana.dd.v1.MsgTouchDID",
  TouchDidLegacy = "/veranablockchain.diddirectory.MsgTouchDID",
}

export enum VeranaCredentialSchemaMessageTypes {
  UpdateParams = "/verana.cs.v1.MsgUpdateParams",
  CreateCredentialSchema = "/verana.cs.v1.MsgCreateCredentialSchema",
  CreateCredentialSchemaLegacy = "/veranablockchain.credentialschema.MsgCreateCredentialSchema",
  UpdateCredentialSchema = "/verana.cs.v1.MsgUpdateCredentialSchema",
  ArchiveCredentialSchema = "/verana.cs.v1.MsgArchiveCredentialSchema",
}

export enum VeranaPermissionMessageTypes {
  UpdateParams = "/verana.perm.v1.MsgUpdateParams",
  CreateRootPermission = "/verana.perm.v1.MsgCreateRootPermission",
  CreatePermission = "/verana.perm.v1.MsgCreatePermission",
  StartPermissionVP = "/verana.perm.v1.MsgStartPermissionVP",
  RenewPermissionVP = "/verana.perm.v1.MsgRenewPermissionVP",
  RevokePermission = "/verana.perm.v1.MsgRevokePermission",
  ExtendPermission = "/verana.perm.v1.MsgExtendPermission",
  SetPermissionVPToValidated = "/verana.perm.v1.MsgSetPermissionVPToValidated",
  CreateOrUpdatePermissionSession = "/verana.perm.v1.MsgCreateOrUpdatePermissionSession",
  SlashPermissionTrustDeposit = "/verana.perm.v1.MsgSlashPermissionTrustDeposit",
  RepayPermissionSlashedTrustDeposit = "/verana.perm.v1.MsgRepayPermissionSlashedTrustDeposit",
  CancelPermissionVPLastRequest = "/verana.perm.v1.MsgCancelPermissionVPLastRequest",
}

export enum VeranaTrustDepositMessageTypes {
  UpdateParams = "/verana.td.v1.MsgUpdateParams",
  ReclaimYield = "/verana.td.v1.MsgReclaimTrustDepositYield",
  ReclaimDeposit = "/verana.td.v1.MsgReclaimTrustDeposit",
  RepaySlashed = "/verana.td.v1.MsgRepaySlashedTrustDeposit",
  SlashTrustDeposit = "/verana.td.v1.MsgSlashTrustDeposit",
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
  DID_DIRECTORY = "/verana.dd.v1.MsgUpdateParams",
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
  ...Object.values(VeranaDidMessageTypes),
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

export function isDidMessageType(messageType: string): boolean {
  return Object.values(VeranaDidMessageTypes).includes(messageType as VeranaDidMessageTypes);
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
  return messageType === VeranaDidMessageTypes.UpdateParams ||
         messageType === VeranaCredentialSchemaMessageTypes.UpdateParams ||
         messageType === VeranaPermissionMessageTypes.UpdateParams ||
         messageType === VeranaTrustDepositMessageTypes.UpdateParams ||
         messageType === VeranaTrustRegistryMessageTypes.UpdateParams;
}