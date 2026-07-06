import { Model } from 'objection'
import BaseModel from './base'
import { CoGovernanceFrameworkVersion } from './co_governance_framework_version'
import { CorporationMember } from './corporation_member'

export class Corporation extends BaseModel {
  static tableName = 'corporation'

  id!: number
  did!: string
  policy_address?: string | null
  corporation?: string | null
  creator?: string | null
  language?: string | null
  group_metadata?: string | null
  group_policy_metadata?: string | null
  decision_policy?: unknown
  doc_url?: string | null
  doc_digest_sri?: string | null
  active_version?: number | null
  created!: Date
  modified!: Date
  height!: number

  members?: CorporationMember[]
  governanceFrameworkVersions?: CoGovernanceFrameworkVersion[]

  static relationMappings = () => ({
    members: {
      relation: Model.HasManyRelation,
      modelClass: CorporationMember,
      join: {
        from: 'corporation.id',
        to: 'corporation_member.corporation_id',
      },
    },
    governanceFrameworkVersions: {
      relation: Model.HasManyRelation,
      modelClass: CoGovernanceFrameworkVersion,
      join: {
        from: 'corporation.id',
        to: 'co_governance_framework_version.corporation_id',
      },
    },
  })
}
