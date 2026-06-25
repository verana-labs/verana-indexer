import { Model } from 'objection'
import BaseModel from './base'
import Participant from './participant'

export interface ParticipantSessionRecord {
  created: string
  issuer_participant_id?: number | null
  verifier_participant_id?: number | null
  wallet_agent_participant_id: number
}

export default class ParticipantSession extends BaseModel {
  static tableName = 'participant_sessions'

  id!: string
  corporation_id!: number
  vs_operator?: string | null
  agent_participant_id!: number
  wallet_agent_participant_id!: number
  session_records!: ParticipantSessionRecord[]
  created!: string
  modified!: string

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['id', 'corporation_id', 'agent_participant_id', 'wallet_agent_participant_id', 'session_records'],
      properties: {
        id: { type: 'string' },
        corporation_id: { type: 'integer' },
        vs_operator: { type: ['string', 'null'] },
        agent_participant_id: { type: 'integer' },
        wallet_agent_participant_id: { type: 'integer' },
        session_records: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              created: { type: 'string' },
              issuer_participant_id: { type: ['integer', 'null'] },
              verifier_participant_id: { type: ['integer', 'null'] },
              wallet_agent_participant_id: { type: 'integer' },
            },
            required: ['created', 'wallet_agent_participant_id'],
          },
        },
        created: { type: 'string' },
        modified: { type: 'string' },
      },
    }
  }

  static get jsonAttributes() {
    return ['session_records']
  }

  static get relationMappings() {
    return {
      agent_participant: {
        relation: Model.BelongsToOneRelation,
        modelClass: Participant,
        join: {
          from: 'participant_sessions.agent_participant_id',
          to: 'participants.id',
        },
      },
      wallet_agent_participant: {
        relation: Model.BelongsToOneRelation,
        modelClass: Participant,
        join: {
          from: 'participant_sessions.wallet_agent_participant_id',
          to: 'participants.id',
        },
      },
    }
  }

  pushSessionRecord(entry: Omit<ParticipantSessionRecord, 'created'> & { created?: string }): void {
    if (!this.session_records) {
      this.session_records = []
    }
    const created = entry.created ?? new Date().toISOString()
    this.session_records.push({
      created,
      issuer_participant_id: entry.issuer_participant_id ?? null,
      verifier_participant_id: entry.verifier_participant_id ?? null,
      wallet_agent_participant_id: entry.wallet_agent_participant_id,
    })
  }
}
