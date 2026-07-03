import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import knex from '../../common/utils/db_connection'

export interface DigestRow {
  digest: string
  created: string | null
}

@Service({
  name: SERVICE.V1.DigestDatabaseService.key,
  version: 1,
})
export default class DigestDatabaseService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({ name: 'syncFromLedger' })
  async syncFromLedger(ctx: {
    params: { digest: DigestRow; blockHeight: number }
  }): Promise<{ success: boolean }> {
    const { digest, blockHeight } = ctx.params

    await knex('digests')
      .insert({
        digest: digest.digest,
        created: digest.created,
        height: blockHeight,
      })
      .onConflict('digest')
      .ignore()

    return { success: true }
  }
}
