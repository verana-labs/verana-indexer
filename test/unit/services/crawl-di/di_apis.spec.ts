jest.mock('../../../../src/models/digest', () => ({ __esModule: true, default: { query: jest.fn() } }))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

jest.mock('../../../../src/common/utils/blockHeight', () => ({
  ...jest.requireActual('../../../../src/common/utils/blockHeight'),
  getResolvedBlockHeight: jest.fn(async (height?: number) => height ?? 777),
}))

import { ServiceBroker } from 'moleculer'
import Digest from '../../../../src/models/digest'
import DigestApiService from '../../../../src/services/crawl-di/di_apis.service'

function digestResolvesTo(row: Record<string, unknown> | undefined) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.first = jest.fn(async () => row)
  ;(Digest as unknown as { query: jest.Mock }).query.mockReturnValue(qb)
  return qb
}

describe('DigestApiService.getDigest', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new DigestApiService(broker)
  const row = { digest: 'sha384-abc', created: '2026-10-01T00:00:00.000Z', height: 100 }

  beforeEach(() => jest.clearAllMocks())

  it('returns the digest with the latest indexed height as block_height', async () => {
    digestResolvesTo(row)

    const res: any = await service.getDigest({ params: { digest: 'sha384-abc' }, meta: {} } as any)

    expect(res.digest).toEqual({ digest: 'sha384-abc', created: '2026-10-01T00:00:00.000Z' })
    expect(res.block_height).toBe(777)
  })

  it('echoes At-Block-Height as block_height and bounds the lookup by that height', async () => {
    const qb = digestResolvesTo(row)

    const res: any = await service.getDigest({ params: { digest: 'sha384-abc' }, meta: { blockHeight: 120 } } as any)

    expect(res.block_height).toBe(120)
    expect(qb.where).toHaveBeenCalledWith('height', '<=', 120)
  })

  it('returns 404 without block_height when the digest is unknown', async () => {
    digestResolvesTo(undefined)

    const res: any = await service.getDigest({ params: { digest: 'sha384-missing' }, meta: {} } as any)

    expect(res).toEqual({ error: 'Digest not found', code: 404 })
  })
})
