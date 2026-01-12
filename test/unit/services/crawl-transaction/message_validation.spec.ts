/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
import { Describe, Test } from '@jest-decorated/core';
import { VeranaDidMessageTypes } from '../../../../src/common/verana-message-types';

@Describe('Test message type validation functionality')
export default class MessageValidationTest {

  @Test('Should identify known Verana message types')
  public testKnownMessageTypes() {
    const knownTypes = new Set([
      ...Object.values(VeranaDidMessageTypes),
    ]);

    expect(knownTypes.has('/verana.dd.v1.MsgAddDID')).toBe(true);
    expect(knownTypes.has('/verana.dd.v1.MsgRenewDID')).toBe(true);
    expect(knownTypes.has('/verana.unknown.v1.MsgUnknown')).toBe(false);
  }

  @Test('Should detect unknown Verana message types')
  public testUnknownMessageTypes() {
    const knownTypes = new Set([
      '/verana.dd.v1.MsgAddDID',
      '/verana.dd.v1.MsgRenewDID',
    ]);

    const unknownMsg = {
      type: '/verana.unknown.v1.MsgUnknownType',
      content: { someField: 'value' }
    };

    const isVeranaMessage = unknownMsg.type.startsWith('/verana.');
    const isKnown = knownTypes.has(unknownMsg.type);

    expect(isVeranaMessage).toBe(true);
    expect(isKnown).toBe(false);
  }

  @Test('Should allow known Verana message types to pass')
  public testKnownMessageTypesPass() {
    const knownTypes = new Set([
      '/verana.dd.v1.MsgAddDID',
      '/verana.dd.v1.MsgRenewDID',
    ]);

    const knownMsg = {
      type: '/verana.dd.v1.MsgAddDID',
      content: { did: 'test-did', years: 1 }
    };

    const isVeranaMessage = knownMsg.type.startsWith('/verana.');
    const isKnown = knownTypes.has(knownMsg.type);

    expect(isVeranaMessage).toBe(true);
    expect(isKnown).toBe(true);
  }

  @Test('Should ignore non-Verana message types')
  public testNonVeranaMessagesIgnored() {
    const knownTypes = new Set([
      '/verana.dd.v1.MsgAddDID',
    ]);

    const nonVeranaMsg = {
      type: '/cosmos.bank.v1beta1.MsgSend',
      content: { from_address: 'test', to_address: 'test2' }
    };

    const isVeranaMessage = nonVeranaMsg.type.startsWith('/verana.');
    const isKnown = knownTypes.has(nonVeranaMsg.type);

    expect(isVeranaMessage).toBe(false);
    expect(isKnown).toBe(false);
  }

  @Test('Should simulate crash behavior for unknown messages')
  public testCrashSimulation() {
    const mockExit = jest.spyOn(process, 'exit').mockImplementation(() => {
      throw new Error('process.exit called');
    });

    const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});

    try {
      const knownVeranaMessageTypes = new Set([
        ...Object.values(VeranaDidMessageTypes),
      ]);

      const messages = [{
        tx_id: 1,
        type: '/verana.unknown.v1.MsgUnknownType',
        content: { unknown_field: 'test' },
      }];

      const transactions = [{
        id: 1,
        height: 1000,
        hash: 'test-tx-hash',
        code: 0,
        timestamp: new Date().toISOString(),
      }];

      const unknownMessages: any[] = [];

      messages.forEach((msg: any) => {
        const isVeranaMessage = msg.type.startsWith('/verana.');
        if (!isVeranaMessage) {
          return;
        }

        if (!knownVeranaMessageTypes.has(msg.type)) {
          unknownMessages.push(msg);
        }
      });

      if (unknownMessages.length > 0) {
        const unknownTypes = [...new Set(unknownMessages.map((msg: any) => msg.type))];

        console.error('='.repeat(80));
        console.error('🚨 CRITICAL: UNKNOWN VERANA MESSAGE TYPES DETECTED');
        console.error('='.repeat(80));
        console.error(`Unknown Verana message types: ${unknownTypes.join(', ')}`);
        console.error(`This indicates a protocol change or new feature that requires indexer updates.`);
        console.error(`Affected transactions: ${unknownMessages.length}`);
        console.error(`Skip mode: DISABLED (PRODUCTION)`);
        console.error('');
        console.error('Sample affected transactions:');
        unknownMessages.slice(0, 3).forEach((msg: any, index: number) => {
          const parentTx = transactions.find((tx) => tx.id === msg.tx_id);
          const height = parentTx?.height ?? 'unknown';
          console.error(`  ${index + 1}. TX ${msg.tx_id} at height ${height}: ${msg.type}`);
        });
        console.error('');
        console.error('🛑 Indexer stopped - update message handlers for new message types');
        console.error('='.repeat(80));

        process.exit(1);
      }

      expect(mockExit).toHaveBeenCalledWith(1);
    } catch (error) {
      expect(error.message).toBe('process.exit called');
      expect(mockConsoleError).toHaveBeenCalledWith('='.repeat(80));
      expect(mockConsoleError).toHaveBeenCalledWith('🚨 CRITICAL: UNKNOWN VERANA MESSAGE TYPES DETECTED');
    } finally {
      mockExit.mockRestore();
      mockConsoleError.mockRestore();
    }
  }

  @Test('Should simulate test mode behavior')
  public testTestModeSimulation() {
    const mockExit = jest.spyOn(process, 'exit').mockImplementation(() => {
      throw new Error('process.exit called');
    });

    const mockConsoleWarn = jest.spyOn(console, 'warn').mockImplementation(() => {});

    try {
      const knownVeranaMessageTypes = new Set([
        ...Object.values(VeranaDidMessageTypes),
      ]);

      const messages = [{
        tx_id: 1,
        type: '/verana.unknown.v1.MsgUnknownType',
        content: { unknown_field: 'test' },
      }];

      const unknownMessages: any[] = [];

      messages.forEach((msg: any) => {
        const isVeranaMessage = msg.type.startsWith('/verana.');
        if (!isVeranaMessage) {
          return;
        }

        if (!knownVeranaMessageTypes.has(msg.type)) {
          unknownMessages.push(msg);
        }
      });

      if (unknownMessages.length > 0) {
        const unknownTypes = [...new Set(unknownMessages.map((msg: any) => msg.type))];
        const skipUnknown = true; // Test mode

        console.warn(`TEST MODE: Skipping validation for unknown Verana message types: ${unknownTypes.join(', ')}`);

        expect(skipUnknown).toBe(true);
        expect(mockExit).not.toHaveBeenCalled();
        expect(mockConsoleWarn).toHaveBeenCalledWith('TEST MODE: Skipping validation for unknown Verana message types: /verana.unknown.v1.MsgUnknownType');
      }
    } finally {
      mockExit.mockRestore();
      mockConsoleWarn.mockRestore();
    }
  }
}
