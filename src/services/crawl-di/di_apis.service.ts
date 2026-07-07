import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import Digest from '../../models/digest'

function serializeDigestRow(row: any) {
  return {
    digest: String(row.digest),
    created: dateToIsoOrNull(row.created),
  }
}

interface GetDigestParams {
  digest: string
}

@Service({
  name: SERVICE.V1.DigestApiService.key,
  version: 1,
})
export default class DigestApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({
    rest: 'GET get/:digest',
    params: {
      digest: { type: 'string', empty: false },
    },
  })
  async getDigest(ctx: Context<GetDigestParams>) {
    try {
      const { digest } = ctx.params

      const blockHeight = getBlockHeight(ctx)
      const query = Digest.query().where('digest', digest)
      if (blockHeight !== undefined) {
        query.where('height', '<=', blockHeight)
      }
      const row = await query.first()

      if (!row) {
        return ApiResponder.error(ctx, 'Digest not found', 404)
      }

      return ApiResponder.success(ctx, {
        digest: serializeDigestRow(row),
      })
    } catch (err: any) {
      this.logger.error('Error in Digest.getDigest:', err)
      return ApiResponder.error(ctx, `Failed to get digest: ${err?.message || String(err)}`, 500)
    }
  }
}
