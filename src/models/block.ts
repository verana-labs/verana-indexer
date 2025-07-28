/* eslint-disable import/no-cycle */
import { Model } from 'objection';
import BaseModel from './base';
import { BlockSignature } from './block_signature';
import { PowerEvent } from './power_event';
import { Transaction } from './transaction';
import { Event } from './event';

export class Block extends BaseModel {
  height!: number;

  hash!: string;

  time!: Date;

  proposer_address!: string;

  data!: any;

  tx_count!: number;

  static get tableName() {
    return 'block';
  }

  static get jsonAttributes() {
    return ['data'];
  }

  static get idColumn(): string | string[] {
    return 'height';
  }

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['height', 'hash', 'time', 'proposer_address', 'data'],
      properties: {
        height: { type: 'number' },
        hash: { type: 'string', minLength: 1, maxLength: 255 },
        time: { type: 'string', format: 'date-time' },
        proposer_address: { type: 'string', minLength: 1, maxLength: 255 },
        tx_count: { type: 'number' },
      },
    };
  }

  static get relationMappings() {
    return {
      signatures: {
        relation: Model.HasManyRelation,
        modelClass: BlockSignature,
        join: {
          from: 'block.height',
          to: 'block_signature.height',
        },
      },
      txs: {
        relation: Model.HasManyRelation,
        modelClass: Transaction,
        join: {
          from: 'block.height',
          to: 'transaction.height',
        },
      },
      power_events: {
        relation: Model.HasManyRelation,
        modelClass: PowerEvent,
        join: {
          from: 'block.height',
          to: 'power_event.height',
        },
      },
      events: {
        relation: Model.HasManyRelation,
        modelClass: Event,
        join: {
          from: 'block.height',
          to: 'event.block_height',
        },
      },
    };
  }
}
