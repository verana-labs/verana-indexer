import { ServiceBroker } from 'moleculer'
import { VeranaCredentialSchemaMessageTypes } from '../../../../src/common/verana-message-types'
import ProcessCredentialSchemaService from '../../../../src/services/crawl-cs/cs_processor.service'

describe('ProcessCredentialSchemaService.handleCredentialSchemas', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(ProcessCredentialSchemaService)

  afterAll(async () => {
    await broker.stop()
  })

  it('fails when a schema cannot be stored instead of logging and moving on', async () => {
    const ctx = {
      params: {
        credentialSchemaMessages: [
          {
            type: VeranaCredentialSchemaMessageTypes.CreateCredentialSchema,
            content: { ecosystem_id: 2, json_schema: '{"title":"T"}' },
            timestamp: '2026-11-13T00:00:00Z',
            height: 100,
          },
        ],
      },
      call: jest.fn().mockRejectedValue(new Error('connection terminated')),
    }

    await expect(service.handleCredentialSchemas(ctx)).rejects.toThrow(
      'Failed to process 1 of 1 credential schema messages'
    )
  })
})
