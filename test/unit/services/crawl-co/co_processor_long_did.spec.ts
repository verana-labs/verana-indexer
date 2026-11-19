import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import { VeranaCorporationMessageTypes } from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

type MessagesContext = Parameters<CorporationMessageProcessorService['handleCorporationMessages']>[0]

// The chain puts no length limit on a DID.
const CORPORATION = 4840
const LONG_DID = `did:example:${'b'.repeat(288)}`

describe('CorporationMessageProcessorService with a DID longer than 255 characters', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(
    CorporationMessageProcessorService
  ) as unknown as CorporationMessageProcessorService

  beforeAll(async () => {
    await knex('corporation_history').where({ corporation_id: CORPORATION }).del()
    await knex('corporation').where({ id: CORPORATION }).del()
  })

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  it('stores the corporation and its history', async () => {
    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaCorporationMessageTypes.CreateCorporation,
            height: 493,
            txEvents: [
              {
                type: 'create_corporation',
                attributes: [
                  { key: 'corporation_id', value: String(CORPORATION) },
                  { key: 'policy_address', value: 'verana1corp484longdid' },
                ],
              },
            ],
            content: { did: LONG_DID, signer: 'verana1signer', language: 'en', timestamp: new Date() },
          },
        ],
      },
    } as unknown as MessagesContext)

    expect(await knex('corporation').where({ id: CORPORATION }).pluck('did')).toEqual([LONG_DID])
    expect(await knex('corporation_history').where({ corporation_id: CORPORATION }).pluck('did')).toEqual([LONG_DID])
  })
})
