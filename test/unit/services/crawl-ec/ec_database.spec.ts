// tests/ecosystemDatabaseService.spec.ts
import { ServiceBroker } from 'moleculer'
import ApiResponder from '../../../../src/common/utils/apiResponse'
import { Ecosystem } from '../../../../src/models/ecosystem'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

jest.mock('../../../../src/models/ecosystem')
jest.mock('../../../../src/common/utils/apiResponse')
jest.mock('../../../../src/services/crawl-pp/pp_state_utils', () => ({
  calculateParticipantState: jest.fn().mockReturnValue('ACTIVE'),
}))
jest.mock('../../../../src/services/crawl-ec/ec_stats', () => ({
  calculateEcosystemStats: jest.fn(),
}))

jest.mock('../../../../src/common/utils/db_connection', () => {
  const mockQuery: any = jest.fn(() => mockQuery)
  mockQuery.whereIn = jest.fn(() => mockQuery)
  mockQuery.select = jest.fn(() => mockQuery)
  mockQuery.where = jest.fn(() => mockQuery)
  mockQuery.orderBy = jest.fn(() => mockQuery)
  mockQuery.limit = jest.fn(() => mockQuery)
  mockQuery.first = jest.fn(() => mockQuery)
  mockQuery.schema = { hasTable: jest.fn().mockResolvedValue(false) }
  return mockQuery
})

describe('EcosystemDatabaseService', () => {
  let broker: ServiceBroker
  let service: EcosystemDatabaseService

  beforeAll(() => {
    broker = new ServiceBroker({ nodeID: 'test-node', logger: false })
    service = new EcosystemDatabaseService(broker)
    broker.createService(EcosystemDatabaseService as any)
  })

  afterAll(() => broker.stop())
  beforeEach(() => jest.clearAllMocks())

  describe('getEcosystem', () => {
    it('should return error if EC not found', async () => {
      ;(Ecosystem.query as any).mockReturnValueOnce({
        findById: jest.fn().mockReturnValueOnce({
          withGraphFetched: jest.fn().mockResolvedValueOnce(undefined),
        }),
      })

      const ctx: any = { params: { ecosystem_id: 1 } }
      await service.getEcosystem(ctx)
      expect(ApiResponder.error).toHaveBeenCalledWith(ctx, 'Ecosystem with id 1 not found', 404)
    })

    it('should return EC with filtered documents for preferred language', async () => {
      const mockTR = {
        toJSON: jest.fn().mockReturnValue({
          id: 1,
          governanceFrameworkVersions: [
            {
              active_since: '2025-01-01',
              documents: [
                { language: 'en', url: 'doc1' },
                { language: 'fr', url: 'doc2' },
              ],
            },
          ],
        }),
      }

      ;(Ecosystem.query as any).mockReturnValueOnce({
        findById: jest.fn().mockReturnValueOnce({
          withGraphFetched: jest.fn().mockResolvedValueOnce(mockTR),
        }),
      })

      const { calculateEcosystemStats } = require('../../../../src/services/crawl-ec/ec_stats')
      ;(calculateEcosystemStats as jest.Mock).mockResolvedValue({
        participants: 0,
        active_schemas: 0,
        archived_schemas: 0,
        weight: 0,
        issued: 0,
        verified: 0,
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: 0,
        ecosystem_slashed_amount_repaid: 0,
        network_slash_events: 0,
        network_slashed_amount: 0,
        network_slashed_amount_repaid: 0,
      })

      const ctx: any = { params: { ecosystem_id: 1, preferred_language: 'en', active_gf_only: 'true' } }
      await service.getEcosystem(ctx)

      expect(ApiResponder.success).toHaveBeenCalled()
      let data = (ApiResponder.success as jest.Mock).mock.calls[0][1]
      data = data?.ecosystem
      expect(data.versions[0].documents).toEqual([{ language: 'en', url: 'doc1' }])
    })
  })

  describe('listEcosystems', () => {
    it('should return error for invalid response_max_size', async () => {
      const ctx: any = { params: { response_max_size: 2000 } }
      await service.listEcosystems(ctx)
      expect(ApiResponder.error).toHaveBeenCalledWith(ctx, 'response_max_size must be between 1 and 1024', 400)
    })

    it('should return filtered list with preferred language and active_gf_only', async () => {
      const mockTR = {
        toJSON: jest.fn().mockReturnValue({
          id: 1,
          governanceFrameworkVersions: [
            {
              active_since: '2025-01-01',
              documents: [
                { language: 'en', url: 'doc1' },
                { language: 'fr', url: 'doc2' },
              ],
            },
          ],
        }),
      }

      // Complete mock for chained calls
      const mockQuery: any = {
        withGraphFetched: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        limit: jest.fn().mockResolvedValue([mockTR]),
      }
      ;(Ecosystem.query as any).mockReturnValue(mockQuery)

      const knex = require('../../../../src/common/utils/db_connection')
      ;(knex.select as jest.Mock).mockResolvedValueOnce([
        {
          id: 1,
          participants: 0,
          active_schemas: 0,
          archived_schemas: 0,
          weight: 0,
          issued: 0,
          verified: 0,
          ecosystem_slash_events: 0,
          ecosystem_slashed_amount: 0,
          ecosystem_slashed_amount_repaid: 0,
          network_slash_events: 0,
          network_slashed_amount: 0,
          network_slashed_amount_repaid: 0,
        },
      ])

      const { calculateEcosystemStats } = require('../../../../src/services/crawl-ec/ec_stats')
      ;(calculateEcosystemStats as jest.Mock).mockResolvedValue({
        participants: 0,
        active_schemas: 0,
        archived_schemas: 0,
        weight: 0,
        issued: 0,
        verified: 0,
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: 0,
        ecosystem_slashed_amount_repaid: 0,
        network_slash_events: 0,
        network_slashed_amount: 0,
        network_slashed_amount_repaid: 0,
      })

      const ctx: any = {
        params: { active_gf_only: 'true', preferred_language: 'fr', response_max_size: 2 },
      }
      await service.listEcosystems(ctx)

      expect(ApiResponder.success).toHaveBeenCalled()
      let data = (ApiResponder.success as jest.Mock).mock.calls[0][1]
      data = data?.ecosystems
      expect(data[0].versions[0].documents).toEqual([{ language: 'fr', url: 'doc2' }])
    })
  })
})
