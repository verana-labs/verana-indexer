import Long from 'long';

export interface IProviderJSClientFactory {
  provider: any;
  cosmwasm: any;
  ibc: any;
  cosmos: any;
}

export interface INetworkInfo {
  chainName: string;
  chainId: string;
  RPC: string[];
  LCD: string[];
  prefixAddress: string;
  databaseName: string;
}

export interface ICoin {
  denom: string;
  amount: string;
}

export interface IPagination {
  limit?: Long;
  key?: Uint8Array;
}

export interface IDelegatorDelegations {
  delegatorAddr: string;
  pagination?: IPagination;
}

export interface IDelegatorRedelegations {
  delegatorAddr: string;
  srcValidatorAddr: string;
  dstValidatorAddr: string;
  pagination?: IPagination;
}

export interface IDelegatorUnbonding {
  delegatorAddr: string;
  pagination?: IPagination;
}

export interface IAllBalances {
  address: string;
  pagination?: IPagination;
}

export interface IValidatorDelegators {
  id: number;
  address: string;
  height: number;
}

export interface IStoreCodes {
  hash: string;
  height: number;
  codeId: Long;
}

export interface IContextStoreCodes {
  codeIds: IStoreCodes[];
}

export interface IContextGetContractInfo {
  addresses: string[];
}

export interface IInstantiateContracts {
  address: string;
  hash: string;
  height: number;
}

export interface IMigrateContracts {
  address: string;
  codeId: string;
}

export interface IContextInstantiateContracts {
  contracts: IInstantiateContracts[];
}

export interface IContextGraphQLQuery {
  operationName: string;
  query: string;
  variables: any;
}

export interface IContextUpdateCw20 {
  cw20Contracts: {
    id: number;
    last_updated_height: number;
  }[];
  startBlock: number;
  endBlock: number;
}

export interface IContextReindexingServiceHistory {
  smartContractId: number;
  startBlock: number;
  endBlock: number;
}
