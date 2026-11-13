import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import IndexerEventsService from '../../../../src/services/api/events.service'
import { subscribeBroadcaster } from '../../../../src/services/api/subscribe_broadcaster'

const HEIGHT = 990_000_000 + Math.floor(Math.random() * 1_000_000)

const row = (messageIndex: number) => ({
  event_type: 'CreateNewCredentialSchema',
  did: 'did:web:block-broadcast.example',
  block_height: HEIGHT,
  tx_hash: `BLOCKBROADCAST${HEIGHT}`,
  tx_index: 0,
  message_index: messageIndex,
  message_type: 'MsgCreateCredentialSchema',
  module: 'cs',
  entity_type: 'CredentialSchema',
  entity_id: String(100 + messageIndex),
  timestamp: new Date('2026-11-13T00:00:00Z'),
  payload: { module: 'cs', tx_index: 0, message_index: messageIndex, entity_id: String(100 + messageIndex) },
})

describe('IndexerEventsService.broadcastBlockIndexed', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(IndexerEventsService)

  afterAll(async () => {
    await knex('indexer_events').where({ block_height: HEIGHT }).del()
    await broker.stop()
    await knex.destroy()
  })

  it('sends every stored event of the block, also when they were stored by an earlier attempt', async () => {
    await knex('indexer_events').insert([row(1), row(0)])
    const broadcast = jest.spyOn(subscribeBroadcaster, 'broadcastBlockEnvelope').mockImplementation(() => undefined)

    await service.broadcastBlockIndexed({ params: { height: HEIGHT, timestamp: '2026-11-13T00:00:00Z' } })

    const envelope = broadcast.mock.calls[0][0] as { block: number; events: Array<{ payload: { entity_id?: string } }> }
    expect(envelope.block).toBe(HEIGHT)
    expect(envelope.events.map((event) => event.payload.entity_id)).toEqual(['100', '101'])
  })
})
