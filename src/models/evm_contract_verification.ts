import BaseModel from './base';

export class EVMContractVerification extends BaseModel {
  id!: number;

  contract_address!: string;

  files!: Buffer;

  creator_tx_hash!: string;

  status!: string;

  abi!: any;

  code_hash!: string;

  created_at!: Date;

  updated_at!: Date;

  compile_detail!: any;

  compiler_version!: string;

  contract_name!: string;

  compiler_setting!: any;

  static get tableName() {
    return 'evm_contract_verification';
  }

  static get idColumn(): string | string[] {
    return 'id';
  }

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['contract_address', 'status'],
      properties: {
        verification_status: {
          type: ['string', 'null'],
          enum: Object.values(this.VERIFICATION_STATUS),
        },
        contract_address: {
          type: 'string',
        },
      },
    };
  }

  static get VERIFICATION_STATUS() {
    return {
      PENDING: 'PENDING',
      FAIL: 'FAIL',
      SUCCESS: 'SUCCESS',
    };
  }

  $beforeInsert() {
    this.created_at = new Date();
    this.updated_at = this.created_at;
  }

  $beforeUpdate() {
    this.updated_at = new Date();
  }
}
