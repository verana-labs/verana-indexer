import { Model } from 'objection'
import BaseModel from './base'
import { CoGovernanceFrameworkDocument } from './co_governance_framework_document'

export class CoGovernanceFrameworkVersion extends BaseModel {
  static tableName = 'co_governance_framework_version'

  id!: number
  corporation_id!: number
  ecosystem_id!: number
  version!: number
  created!: Date
  active_since?: Date | null

  documents?: CoGovernanceFrameworkDocument[]

  static relationMappings = () => ({
    documents: {
      relation: Model.HasManyRelation,
      modelClass: CoGovernanceFrameworkDocument,
      join: {
        from: 'co_governance_framework_version.id',
        to: 'co_governance_framework_document.gfv_id',
      },
    },
  })
}
