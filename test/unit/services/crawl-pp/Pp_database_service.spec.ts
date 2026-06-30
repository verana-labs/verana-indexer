import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ParticipantIngestService from '../../../../src/services/crawl-pp/pp_database.service'

// Mock knex
jest.mock('../../../../src/common/utils/db_connection', () => {
  const mockQuery: any = jest.fn(() => mockQuery)
  mockQuery.where = jest.fn(() => mockQuery)
  mockQuery.select = jest.fn(() => mockQuery)
  mockQuery.first = jest.fn(() => mockQuery)
  mockQuery.insert = jest.fn(() => mockQuery)
  mockQuery.update = jest.fn(() => mockQuery)
  mockQuery.transaction = jest.fn((fn) => fn(mockQuery))
  mockQuery.commit = jest.fn()
  mockQuery.rollback = jest.fn()
  mockQuery.returning = jest.fn().mockResolvedValue([{}])
  mockQuery.raw = jest.fn().mockResolvedValue({ rowCount: 0 })
  mockQuery.schema = {
    hasColumn: jest.fn().mockResolvedValue(false),
    hasTable: jest.fn().mockResolvedValue(true),
  }
  return mockQuery
})

jest.mock('../../../../src/common/utils/date_utils', () => ({
  formatTimestamp: jest.fn((v) => `formatted-${v}`),
}))

describe('🧪 ParticipantIngestService Unit Tests', () => {
  let broker: ServiceBroker
  let service: any
  let syncParticipantFromLedger: any
  let mapLedgerParticipantToDbRow: (row: Record<string, any>) => Record<string, any>

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false })
    service = broker.createService(ParticipantIngestService)
    syncParticipantFromLedger = (service as any).syncParticipantFromLedger.bind(service)
    mapLedgerParticipantToDbRow = (service as any).mapLedgerParticipantToDbRow.bind(service)
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('handleMsgCreateRootParticipant', () => {
    it('should insert participant with proper fields', async () => {
      ;(knex.insert as jest.Mock).mockResolvedValueOnce([1])

      const msg = {
        schema_id: 99,
        did: 'did:test:123',
        creator: 'grantee1',
        corporation_id: 5,
        timestamp: '2025-10-08T00:00:00Z',
        effective_from: '2025-10-09T00:00:00Z',
        effective_until: '2025-12-31T00:00:00Z',
        validation_fees: 10,
        issuance_fees: 5,
        verification_fees: 2,
        country: 'PK',
      }

      await service.handleCreateRootParticipant(msg)

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          schema_id: 99,
          role: 'ECOSYSTEM',
          did: 'did:test:123',
          corporation_id: 5,
          validation_fees: 10,
          issuance_fees: 5,
          verification_fees: 2,
        })
      )
    })

    it('should skip insert if schema_id is missing', async () => {
      const msg = { creator: 'grantee1' }
      await service.handleCreateRootParticipant(msg as any)
      expect(knex.insert).not.toHaveBeenCalled()
    })
  })

  describe('handleMsgSelfCreateParticipant', () => {
    it('should insert new participant if root ecosystem exists', async () => {
      ;(knex.where as jest.Mock).mockReturnValueOnce({
        first: jest.fn().mockResolvedValue({ id: 1 }),
      })
      ;(knex.insert as jest.Mock).mockResolvedValueOnce([2])

      const msg = {
        schema_id: 99,
        did: 'did:test:123',
        creator: 'issuer1',
        corporation_id: 7,
        role: 1,
      }

      await service.handleCreateParticipant(msg)

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          validator_participant_id: 1,
          schema_id: 99,
          corporation_id: 7,
        })
      )
    })
  })

  describe('handleRevokeParticipant', () => {
    it('should revoke participant if caller is grantee', async () => {
      ;(knex.first as jest.Mock)
        .mockResolvedValueOnce({ id: 10, corporation_id: 3, schema_id: 1 })
        .mockResolvedValueOnce({ id: 3 })
      ;(knex.transaction as jest.Mock).mockImplementation((fn) => fn(knex))

      const result = await service.handleRevokeParticipant({
        id: 10,
        creator: 'user1',
        timestamp: 'now',
      })

      expect(knex.update).toHaveBeenCalled()
      expect(result.success).toBe(true)
    })

    it('should return error if participant not found', async () => {
      ;(knex.first as jest.Mock).mockResolvedValueOnce(null)
      const result = await service.handleRevokeParticipant({
        id: 999,
        creator: 'user1',
      })
      expect(result.success).toBe(false)
    })
  })

  describe('handleStartParticipantOP', () => {
    it('should insert new OP record', async () => {
      ;(knex.first as jest.Mock).mockResolvedValueOnce({
        id: 99,
        schema_id: 99,
        validation_fees: 0,
      })

      const msg = {
        validator_participant_id: 99,
        did: 'did:test:abc',
        creator: 'alice',
        timestamp: 't1',
      }

      await service.handleStartParticipantOP(msg)

      expect(knex.insert).toHaveBeenCalledWith(
        expect.objectContaining({
          validator_participant_id: 99,
          op_state: 'PENDING',
        })
      )
    })
  })

  describe('mapLedgerParticipantToDbRow (VPR v4 corporation_id)', () => {
    it('maps corporation_id from ledger', () => {
      const row = mapLedgerParticipantToDbRow({
        id: 1,
        schema_id: 2,
        role: 'ECOSYSTEM',
        did: 'did:example:x',
        corporation_id: 7,
      })
      expect(row.corporation_id).toBe(7)
    })

    it('defaults corporation_id to 0 when missing (NOT NULL column)', () => {
      const row = mapLedgerParticipantToDbRow({
        id: 1,
        schema_id: 2,
        role: 'ECOSYSTEM',
        did: 'did:x',
      })
      expect(row.corporation_id).toBe(0)
    })
  })

  describe('syncParticipantFromLedger vs legacy stats parity', () => {
    it('should route through same stats helpers for participants/weight', async () => {
      const mockTrx: any = knex
      const updateWeightSpy = jest.spyOn(service as any, 'updateWeight').mockResolvedValue(undefined)
      const updateParticipantsSpy = jest.spyOn(service as any, 'updateParticipants').mockResolvedValue(undefined)

      ;(knex.first as jest.Mock).mockResolvedValueOnce(null)
      ;(knex.insert as jest.Mock).mockResolvedValueOnce([
        {
          id: 7,
          schema_id: 48,
        },
      ])

      const ledgerParticipant = {
        id: '7',
        schema_id: '48',
        role: 'ISSUER',
        did: 'did:test:issuer',
        corporation_id: 5,
        created: '2026-01-29T20:27:06.725Z',
        modified: '2026-01-29T20:27:23.422Z',
        effective_from: '2026-01-29T20:27:23.422Z',
        effective_until: null,
        op_state: 'VALIDATED',
      }

      await syncParticipantFromLedger(
        ledgerParticipant,
        1908620,
        'tx-hash',
        '/verana.pp.v1.MsgSetParticipantOPToValidated'
      )

      expect(updateWeightSpy).toHaveBeenCalledWith(mockTrx, 7)
      expect(updateParticipantsSpy).toHaveBeenCalledWith(mockTrx, 7)
    })
  })
})
