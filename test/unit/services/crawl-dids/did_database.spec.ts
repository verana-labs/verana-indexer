import DidDatabaseService from '../../../../src/services/crawl-dids/dids.service';
import { ServiceBroker } from 'moleculer';

// Mock knex
jest.mock('../../../../src/common/utils/db_connection', () => {
    return jest.fn(); // knex will be a mock function
});

import knex from '../../../../src/common/utils/db_connection';

describe('DidDatabaseService', () => {
    const broker = new ServiceBroker({ logger: false });
    const service = new DidDatabaseService(broker);

    beforeAll(() => broker.start());
    afterAll(() => broker.stop());

    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('upsertProcessedDid', () => {
        it('should insert a new DID record', async () => {
            const params = { did: 'did:example:123', event_type: 'add_did' };

            const mergeMock = jest.fn().mockResolvedValue(1);
            const onConflictMock = jest.fn(() => ({ merge: mergeMock }));
            (knex as jest.Mock).mockImplementation(() => ({
                insert: jest.fn(() => ({ onConflict: onConflictMock })),
            }));

            const result = await service.upsertProcessedDid({ params });
            expect(knex).toHaveBeenCalledWith('dids');
            expect(result).toBe(1);
        });
    });

    describe('deleteDid', () => {
        it('should mark DID as deleted', async () => {
            const updateMock = jest.fn().mockResolvedValue(1);
            (knex as jest.Mock).mockImplementation(() => ({
                where: jest.fn(() => ({ update: updateMock })),
            }));

            const result = await service.deleteDid({ params: { did: 'did:example:123' } });
            expect(knex).toHaveBeenCalledWith('dids');
            expect(result).toEqual({ success: true });
        });

        it('should return failure if DID does not exist', async () => {
            const updateMock = jest.fn().mockResolvedValue(0);
            (knex as jest.Mock).mockImplementation(() => ({
                where: jest.fn(() => ({ update: updateMock })),
            }));

            const result = await service.deleteDid({ params: { did: 'did:example:notfound' } });
            expect(result).toEqual({
                success: false,
                message: 'No record found for DID: did:example:notfound',
            });
        });
    });

    describe('getDid', () => {
        it('should return a DID record if found', async () => {
            const record = { did: 'did:example:123', event_type: 'add_did' };
            (knex as jest.Mock).mockImplementation(() => ({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(record) })),
            }));

            const result = await service.getDid({ params: { did: 'did:example:123' } });
            expect(result).toEqual(record);
        });

        it('should return undefined if DID is not found', async () => {
            (knex as jest.Mock).mockImplementation(() => ({
                where: jest.fn(() => ({ first: jest.fn().mockResolvedValue(undefined) })),
            }));

            const result = await service.getDid({ params: { did: 'did:example:notfound' } });
            expect(result).toBeUndefined();
        });
    });
});
