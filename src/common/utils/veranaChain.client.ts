import { TsProtoGeneratedType } from '@cosmjs/proto-signing';
import { MsgArchiveCredentialSchema, MsgCreateCredentialSchema, MsgUpdateCredentialSchema } from "@verana-labs/verana-types/codec/verana/cs/v1/tx";
import { MsgAddDID, MsgRemoveDID, MsgRenewDID, MsgTouchDID } from '@verana-labs/verana-types/codec/verana/dd/v1/tx';
import { MsgCancelPermissionVPLastRequest, MsgCreateOrUpdatePermissionSession, MsgCreatePermission, MsgCreateRootPermission, MsgExtendPermission, MsgRenewPermissionVP, MsgRepayPermissionSlashedTrustDeposit as MsgRepayPermissionSlashedTrustDepositPerm, MsgRevokePermission, MsgSetPermissionVPToValidated, MsgSlashPermissionTrustDeposit, MsgStartPermissionVP } from '@verana-labs/verana-types/codec/verana/perm/v1/tx';
import { MsgReclaimTrustDeposit, MsgReclaimTrustDepositYield, MsgRepaySlashedTrustDeposit } from '@verana-labs/verana-types/codec/verana/td/v1/tx';
import { MsgAddGovernanceFrameworkDocument, MsgArchiveTrustRegistry, MsgCreateTrustRegistry, MsgIncreaseActiveGovernanceFrameworkVersion, MsgUpdateTrustRegistry } from '@verana-labs/verana-types/codec/verana/tr/v1/tx';


export const veranaRegistry: readonly [string, TsProtoGeneratedType][] = [
    // verana.dd.v1
    ["/verana.dd.v1.MsgAddDID", MsgAddDID],
    ["/veranablockchain.diddirectory.MsgAddDID", MsgAddDID],
    ["/verana.dd.v1.MsgRenewDID", MsgRenewDID],
    ["/veranablockchain.diddirectory.MsgRenewDID", MsgRenewDID],
    ["/verana.dd.v1.MsgTouchDID", MsgTouchDID],
    ["/veranablockchain.diddirectory.MsgTouchDID", MsgTouchDID],
    ["/verana.dd.v1.MsgRemoveDID", MsgRemoveDID],
    ["/veranablockchain.diddirectory.MsgRemoveDID", MsgRemoveDID],
    // verana.td.v1
    ["/verana.td.v1.MsgReclaimTrustDepositYield", MsgReclaimTrustDepositYield],
    ["/verana.td.v1.MsgReclaimTrustDeposit", MsgReclaimTrustDeposit],
    ["/verana.td.v1.MsgRepaySlashedTrustDeposit", MsgRepaySlashedTrustDeposit],
    // verana.tr.v1
    ["/verana.tr.v1.MsgCreateTrustRegistry", MsgCreateTrustRegistry],
    ["/veranablockchain.trustregistry.MsgCreateTrustRegistry", MsgCreateTrustRegistry],
    ["/verana.tr.v1.MsgUpdateTrustRegistry", MsgUpdateTrustRegistry],
    ["/verana.tr.v1.MsgArchiveTrustRegistry", MsgArchiveTrustRegistry],
    ["/verana.tr.v1.MsgAddGovernanceFrameworkDocument", MsgAddGovernanceFrameworkDocument],
    ["/verana.tr.v1.MsgIncreaseActiveGovernanceFrameworkVersion", MsgIncreaseActiveGovernanceFrameworkVersion],
    // verana.cs.v1
    ["/verana.cs.v1.MsgCreateCredentialSchema", MsgCreateCredentialSchema],
    ["/veranablockchain.credentialschema.MsgCreateCredentialSchema", MsgCreateCredentialSchema],
    ["/verana.cs.v1.MsgUpdateCredentialSchema", MsgUpdateCredentialSchema],
    ["/verana.cs.v1.MsgArchiveCredentialSchema", MsgArchiveCredentialSchema],
    // verana.perm.v1
    ["/verana.perm.v1.MsgStartPermissionVP", MsgStartPermissionVP],
    ["/verana.perm.v1.MsgRenewPermissionVP", MsgRenewPermissionVP],
    ["/verana.perm.v1.MsgSetPermissionVPToValidated", MsgSetPermissionVPToValidated],
    ["/verana.perm.v1.MsgCancelPermissionVPLastRequest", MsgCancelPermissionVPLastRequest],
    ["/verana.perm.v1.MsgCreateRootPermission", MsgCreateRootPermission],
    ["/verana.perm.v1.MsgExtendPermission", MsgExtendPermission],
    ["/verana.perm.v1.MsgRevokePermission", MsgRevokePermission],
    ["/verana.perm.v1.MsgCreateOrUpdatePermissionSession", MsgCreateOrUpdatePermissionSession],
    ["/verana.perm.v1.MsgSlashPermissionTrustDeposit", MsgSlashPermissionTrustDeposit],
    ["/verana.perm.v1.MsgRepayPermissionSlashedTrustDeposit", MsgRepayPermissionSlashedTrustDepositPerm],
    ["/verana.perm.v1.MsgCreatePermission", MsgCreatePermission]
];


