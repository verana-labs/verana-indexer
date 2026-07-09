import BaseModel from './base'

export class CoGovernanceFrameworkDocument extends BaseModel {
  static tableName = 'co_governance_framework_document'

  id!: number
  gfv_id!: number
  language!: string
  url!: string
  digest_sri!: string
  created!: Date
  gfd_id?: number | null
}
