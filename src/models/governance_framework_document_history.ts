import { Model } from 'objection'
import BaseModel from './base'
import { Ecosystem } from './ecosystem'
import { GovernanceFrameworkDocument } from './governance_framework_document'
import { GovernanceFrameworkVersion } from './governance_framework_version'

export class GovernanceFrameworkDocumentHistory extends BaseModel {
  static tableName = 'governance_framework_document_history'

  id!: number
  gfd_id!: number
  gfv_id!: number
  ecosystem_id!: number
  created!: Date
  language!: string
  url!: string
  digest_sri!: string
  event_type!: string
  height!: number
  changes?: Record<string, any>
  created_at!: Date

  static relationMappings = () => ({
    governanceFrameworkDocument: {
      relation: Model.BelongsToOneRelation,
      modelClass: GovernanceFrameworkDocument,
      join: {
        from: 'governance_framework_document_history.gfd_id',
        to: 'governance_framework_document.id',
      },
    },
    governanceFrameworkVersion: {
      relation: Model.BelongsToOneRelation,
      modelClass: GovernanceFrameworkVersion,
      join: {
        from: 'governance_framework_document_history.gfv_id',
        to: 'governance_framework_version.id',
      },
    },
    ecosystem: {
      relation: Model.BelongsToOneRelation,
      modelClass: Ecosystem,
      join: {
        from: 'governance_framework_document_history.ecosystem_id',
        to: 'ecosystem.id',
      },
    },
  })
}
