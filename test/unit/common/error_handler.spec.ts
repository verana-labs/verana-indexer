import { describe, expect, it } from '@jest/globals';
import { analyzeError } from '../../../src/common/utils/error_handler';

describe('analyzeError', () => {
  it('does not stop the indexer on a duplicate-key block insert whose SQL contains $500..$504 placeholders', () => {
    const message =
      'insert into "block" ("data","hash","height","proposer_address","time","tx_count") values ' +
      '($499, $500, $501, $502, $503, $504) returning "height" - ' +
      'duplicate key value violates unique constraint "block_pkey"';
    const error: any = new Error(message);
    error.code = '23505';

    const info = analyzeError(error);

    expect(info.isServerError).toBe(false);
    expect(info.shouldStopIndexer).toBe(false);
  });

  it('treats a unique-constraint violation as recoverable regardless of message content', () => {
    const error: any = new Error('duplicate key value violates unique constraint "block_pkey"');
    error.code = '23505';
    expect(analyzeError(error).shouldStopIndexer).toBe(false);
  });

  it('does not misclassify arbitrary numeric content in a message as a 5xx server error', () => {
    const info = analyzeError(new Error('processed block 500123 with a warning'));
    expect(info.isServerError).toBe(false);
    expect(info.shouldStopIndexer).toBe(false);
  });

  it('still stops the indexer on a real HTTP 5xx by status code', () => {
    const error: any = new Error('Request failed');
    error.response = { status: 502, statusText: 'Bad Gateway' };
    const info = analyzeError(error);
    expect(info.isServerError).toBe(true);
    expect(info.shouldStopIndexer).toBe(true);
  });

  it('classifies any 5xx status code as a server error', () => {
    for (const status of [500, 502, 503, 504, 599]) {
      const error: any = new Error('Request failed');
      error.response = { status };
      expect(analyzeError(error).isServerError).toBe(true);
    }
  });

  it('does not classify a 4xx status code as a server error', () => {
    const error: any = new Error('Not Found');
    error.response = { status: 404, statusText: 'Not Found' };
    const info = analyzeError(error);
    expect(info.isServerError).toBe(false);
    expect(info.shouldStopIndexer).toBe(false);
  });

  it('bounds server errors to the 5xx range and ignores out-of-range status codes', () => {
    const highest5xx: any = new Error('Server error');
    highest5xx.response = { status: 599 };
    expect(analyzeError(highest5xx).isServerError).toBe(true);

    for (const status of [600, 700, 880]) {
      const error: any = new Error('Weird status');
      error.response = { status };
      const info = analyzeError(error);
      expect(info.isServerError).toBe(false);
      expect(info.shouldStopIndexer).toBe(false);
    }
  });

  it('still classifies textual server errors', () => {
    expect(analyzeError(new Error('Internal Server Error')).isServerError).toBe(true);
    expect(analyzeError(new Error('upstream returned Bad Gateway')).isServerError).toBe(true);
  });
});
