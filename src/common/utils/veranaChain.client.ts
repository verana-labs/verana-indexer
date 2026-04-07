import { TsProtoGeneratedType } from '@cosmjs/proto-signing';
import { MsgArchiveCredentialSchema, MsgCreateCredentialSchema, MsgUpdateCredentialSchema, MsgUpdateParams as MsgUpdateParamsCS } from "@verana-labs/verana-types/codec/verana/cs/v1/tx";
import { MsgCancelPermissionVPLastRequest, MsgCreateOrUpdatePermissionSession, MsgCreatePermission, MsgCreateRootPermission, MsgRenewPermissionVP, MsgRepayPermissionSlashedTrustDeposit as MsgRepayPermissionSlashedTrustDepositPerm, MsgRevokePermission, MsgSetPermissionVPToValidated, MsgSlashPermissionTrustDeposit, MsgStartPermissionVP, MsgUpdateParams as MsgUpdateParamsPerm } from '@verana-labs/verana-types/codec/verana/perm/v1/tx';
import { MsgReclaimTrustDeposit, MsgReclaimTrustDepositYield, MsgRepaySlashedTrustDeposit, MsgUpdateParams } from '@verana-labs/verana-types/codec/verana/td/v1/tx';
import { MsgAddGovernanceFrameworkDocument, MsgArchiveTrustRegistry, MsgCreateTrustRegistry, MsgIncreaseActiveGovernanceFrameworkVersion, MsgUpdateTrustRegistry, MsgUpdateParams as MsgUpdateParamsTR } from '@verana-labs/verana-types/codec/verana/tr/v1/tx';
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from '../verana-message-types';

export const veranaRegistry: readonly [string, TsProtoGeneratedType][] = [
    // verana.td.v1
    [VeranaTrustDepositMessageTypes.ReclaimYield, MsgReclaimTrustDepositYield],
    [VeranaTrustDepositMessageTypes.ReclaimDeposit, MsgReclaimTrustDeposit],
    [VeranaTrustDepositMessageTypes.RepaySlashed, MsgRepaySlashedTrustDeposit],
    [VeranaTrustDepositMessageTypes.UpdateParams, MsgUpdateParams],
    // verana.tr.v1
    [VeranaTrustRegistryMessageTypes.CreateTrustRegistry, MsgCreateTrustRegistry],
    [VeranaTrustRegistryMessageTypes.UpdateTrustRegistry, MsgUpdateTrustRegistry],
    [VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry, MsgArchiveTrustRegistry],
    [VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc, MsgAddGovernanceFrameworkDocument],
    [VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion, MsgIncreaseActiveGovernanceFrameworkVersion],
    [VeranaTrustRegistryMessageTypes.UpdateParams, MsgUpdateParamsTR],
    // verana.cs.v1
    [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema, MsgCreateCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema, MsgUpdateCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema, MsgArchiveCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.UpdateParams, MsgUpdateParamsCS],
    // verana.perm.v1
    [VeranaPermissionMessageTypes.StartPermissionVP, MsgStartPermissionVP],
    [VeranaPermissionMessageTypes.RenewPermissionVP, MsgRenewPermissionVP],
    [VeranaPermissionMessageTypes.SetPermissionVPToValidated, MsgSetPermissionVPToValidated],
    [VeranaPermissionMessageTypes.CancelPermissionVPLastRequest, MsgCancelPermissionVPLastRequest],
    [VeranaPermissionMessageTypes.CreateRootPermission, MsgCreateRootPermission],
    [VeranaPermissionMessageTypes.RevokePermission, MsgRevokePermission],
    [VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession, MsgCreateOrUpdatePermissionSession],
    [VeranaPermissionMessageTypes.SlashPermissionTrustDeposit, MsgSlashPermissionTrustDeposit],
    [VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit, MsgRepayPermissionSlashedTrustDepositPerm],
    [VeranaPermissionMessageTypes.CreatePermission, MsgCreatePermission],
    [VeranaPermissionMessageTypes.UpdateParams, MsgUpdateParamsPerm]
];


