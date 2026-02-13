import { Registry, TsProtoGeneratedType } from '@cosmjs/proto-signing';
import { defaultRegistryTypes as defaultStargateTypes } from '@cosmjs/stargate';
import { wasmTypes } from '@cosmjs/cosmwasm-stargate/build/modules';
import { toBase64, fromUtf8, fromBase64, toUtf8 } from '@cosmjs/encoding';
import { LoggerInstance } from 'moleculer';
import _ from 'lodash';
import { SemVer } from 'semver';
import { MSG_TYPE } from '../index';
import Utils from './utils';
import { IProviderRegistry } from './provider.registry';
import { veranaRegistry } from './veranaChain.client';

interface MessageWithTypeUrl {
  typeUrl: string;
  value?: string | Uint8Array;
  [key: string]: unknown;
}

export default class ChainRegistry {
  public registry!: Registry;

  private _logger: LoggerInstance;

  public cosmos: unknown;

  public ibc: unknown;

  public ethermint: unknown;

  public seiprotocol: unknown;

  public aura: unknown;

  public evmos: unknown;

  public cosmosSdkVersion: SemVer = new SemVer('v0.45.17');

  public decodeAttribute: ((input: string) => string) | undefined;

  public encodeAttribute: ((input: string) => string) | undefined;

  public txRegistryType: unknown;

  constructor(logger: LoggerInstance, providerRegistry: IProviderRegistry) {
    this._logger = logger;
    this.cosmos = providerRegistry.cosmos;
    this.ibc = providerRegistry.ibc;
    this.txRegistryType = providerRegistry.txRegistryType;
    this.aura = providerRegistry.aura;
    this.seiprotocol = providerRegistry.seiprotocol;
    this.evmos = providerRegistry.evmos;
    this.ethermint = providerRegistry.ethermint;

    // set default registry to decode msg
    const txRegistryTypes = Array.isArray(this.txRegistryType) 
      ? (this.txRegistryType as string[]).map((type: string) => [
          type,
          _.get(this, type.slice(1)),
        ])
      : [];
    this.registry = new Registry([
      ...defaultStargateTypes,
      ...wasmTypes,
      ...veranaRegistry,
      ...txRegistryTypes,
    ] as any);
  }

  public decodeMsg(msg: unknown): unknown {
    this._logger.warn("Decoding msg:", msg);
    let result: Record<string, unknown> = {};
    if (!msg) {
      return result;
    }
    
    const typedMsg = msg as MessageWithTypeUrl;
    if (typedMsg && typeof typedMsg === 'object' && 'typeUrl' in typedMsg && typeof typedMsg.typeUrl === 'string') {
      result['@type'] = typedMsg.typeUrl;
      const msgType = this.registry.lookupType(
        typedMsg.typeUrl
      ) as TsProtoGeneratedType;
      this._logger.warn("msgType", msgType);
      if (!msgType) {
        const formattedValue =
          typedMsg.value instanceof Uint8Array ? toBase64(typedMsg.value) : typedMsg.value;
        result.value = formattedValue;
        
        const isVeranaMessage = typedMsg.typeUrl.startsWith('/verana.');
        const isCosmosSystemMessage = typedMsg.typeUrl.includes('.upgrade.') || 
                                      typedMsg.typeUrl.includes('.gov.') ||
                                      typedMsg.typeUrl.includes('.consensus.');
        
        if (isVeranaMessage) {
          this._logger.error('Unsupported Verana message type:', typedMsg.typeUrl);
          this._logger.error('This may indicate a protocol change requiring indexer updates');
        } else if (isCosmosSystemMessage) {
          this._logger.debug('Cosmos SDK system message (not processed):', typedMsg.typeUrl);
        } else {
          this._logger.warn('Unknown message type (not Verana):', typedMsg.typeUrl);
        }
      } else {
        const msgValue = typedMsg.value;
        let decodedValue: Uint8Array;
        if (msgValue instanceof Uint8Array) {
          decodedValue = msgValue;
        } else if (typeof msgValue === 'string' && Utils.isBase64(msgValue)) {
          decodedValue = fromBase64(msgValue);
        } else if (typeof msgValue === 'string') {
          decodedValue = new TextEncoder().encode(msgValue);
        } else {
          decodedValue = new Uint8Array();
        }
        const decoded: Record<string, unknown> = msgType.toJSON(
          this.registry.decode({
            typeUrl: typedMsg.typeUrl,
            value: decodedValue,
          } as { typeUrl: string; value: Uint8Array })
        ) as Record<string, unknown>;
        Object.keys(decoded).forEach((key) => {
          const value = decoded[key];
          if (value && typeof value === 'object' && value !== null && 'typeUrl' in value) {
            const resultRecursive = this.decodeMsg(value as MessageWithTypeUrl);
            result[key] = resultRecursive;
          } else {
            result[key] = decoded[key];
          }
        });
      }

      if (
        typedMsg.typeUrl === MSG_TYPE.MSG_EXECUTE_CONTRACT ||
        typedMsg.typeUrl === MSG_TYPE.MSG_INSTANTIATE_CONTRACT ||
        typedMsg.typeUrl === MSG_TYPE.MSG_INSTANTIATE2_CONTRACT
      ) {
        if (result.msg && typeof result.msg === 'string') {
          try {
            result.msg = fromUtf8(fromBase64(result.msg));
          } catch (error) {
            this._logger.error('This msg instantite/execute is not valid JSON');
          }
        }
      } else if (typedMsg.typeUrl === MSG_TYPE.MSG_ACKNOWLEDGEMENT) {
        try {
          const packet = result.packet as { data?: string } | undefined;
          const acknowledgement = result.acknowledgement;
          if (packet?.data && typeof packet.data === 'string') {
            result.packet = {
              ...packet,
              data: JSON.parse(
                fromUtf8(fromBase64(packet.data))
              ),
            };
          }
          if (acknowledgement && typeof acknowledgement === 'string') {
            result.acknowledgement = JSON.parse(
              fromUtf8(fromBase64(acknowledgement))
            );
          }
        } catch (error) {
          this._logger.error('This msg ibc acknowledgement is not valid JSON');
        }
      } else if (typedMsg.typeUrl === MSG_TYPE.MSG_AUTHZ_EXEC) {
        try {
          const msgs = result.msgs as Array<{ typeUrl: string; value: string | Uint8Array }> | undefined;
          if (Array.isArray(msgs)) {
            result.msgs = msgs.map((subMsg) => {
              const valueStr = typeof subMsg.value === 'string' ? subMsg.value : String(subMsg.value);
              return this.decodeMsg({
                typeUrl: subMsg.typeUrl,
                value: Utils.isBase64(valueStr)
                  ? fromBase64(valueStr)
                  : subMsg.value,
              });
            });
          }
        } catch (error) {
          this._logger.error('Cannot decoded sub messages authz exec');
        }
      } else if (typedMsg.typeUrl === MSG_TYPE.MSG_SUBMIT_PROPOSAL_V1) {
        try {
          const messages = result.messages as Array<{ typeUrl: string; value: string | Uint8Array }> | undefined;
          if (Array.isArray(messages)) {
            result.messages = messages.map((subMsg) => {
              const valueStr = typeof subMsg.value === 'string' ? subMsg.value : String(subMsg.value);
              return this.decodeMsg({
                typeUrl: subMsg.typeUrl,
                value: Utils.isBase64(valueStr)
                  ? fromBase64(valueStr)
                  : subMsg.value,
              });
            });
          }
        } catch (error) {
          this._logger.error('Cannot decoded sub messages in proposal');
        }
      }
    } else {
      result = msg as Record<string, unknown>;
    }

    return result;
  }

  public addTypes(types: string[]) {
    types.forEach((type) =>
      this.registry.register(type, _.get(this, type.slice(1)))
    );
  }

  public setCosmosSdkVersionByString(version: string) {
    this.cosmosSdkVersion = new SemVer(version);

    if (this.cosmosSdkVersion.compare('v0.45.99') === -1) {
      this.decodeAttribute = (input: string) => {
        if (!input) {
          return input;
        }
        try {
          return fromUtf8(fromBase64(input));
        } catch (error) {
          this._logger.error(error);
          return input;
        }
      };
      this.encodeAttribute = (input: string) => toBase64(toUtf8(input));
    } else {
      this.decodeAttribute = (input: string) => input;
      this.encodeAttribute = (input: string) => input;
    }
  }
}
