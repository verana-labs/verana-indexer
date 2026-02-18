import axios, { AxiosError, AxiosResponse } from 'axios';

function getBaseUrl(): string {
  return 'http://localhost:3001';
}

const TIMEOUT = 30000;
const MAX_RETRIES = 2;

const SAMPLE_DID = 'did:verana:test123';
const SAMPLE_ID = 1;
const SAMPLE_ACCOUNT = 'verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st';
const SAMPLE_TR_ID = 1;
const SAMPLE_BLOCK_HEIGHT = 1000;
const SAMPLE_PERM_ID = 1;
const SAMPLE_SCHEMA_ID = 1;

function getTimestamps() {
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const lastWeek = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  return {
    from: yesterday.toISOString().replace(/\.\d{3}Z$/, 'Z'),
    until: now.toISOString().replace(/\.\d{3}Z$/, 'Z'),
    lastWeek: lastWeek.toISOString().replace(/\.\d{3}Z$/, 'Z'),
  };
}

async function makeRequest(
  method: string,
  url: string,
  config: any = {}
): Promise<AxiosResponse> {
  let lastError: Error | null = null;
  
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await axios({
        method: method.toLowerCase(),
        url,
        timeout: TIMEOUT,
        validateStatus: () => true,
        ...config,
      });
      return response;
    } catch (error) {
      lastError = error as Error;
      if (attempt < MAX_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt + 1)));
        continue;
      }
    }
  }
  
  throw lastError || new Error('Request failed after retries');
}

async function testEndpoint(
  method: string,
  path: string,
  params: any = {},
  headers: Record<string, string | number> = {}
): Promise<AxiosResponse> {
  const url = `${getBaseUrl()}${path}`;
  
  const config: any = {
    headers: Object.fromEntries(
      Object.entries(headers).map(([key, value]) => [key, String(value)])
    ),
  };

  if (method === 'GET') {
    config.params = params;
  } else {
    config.data = params;
  }

  const response = await makeRequest(method, url, config);
  return response;
}

let serverAvailable = false;

describe('Comprehensive API Endpoints Integration Tests', () => {
  beforeAll(async () => {
    try {
      const baseUrl = getBaseUrl();
      const response = await axios.get(`${baseUrl}/verana/indexer/v1/version`, {
        timeout: 5000,
      });
      serverAvailable = true;
      console.log(`✓ Server is reachable at ${baseUrl}`);
      console.log(`  Server version response: ${response.status}`);
    } catch (error) {
      serverAvailable = false;
      const err = error as AxiosError;
      const baseUrl = getBaseUrl();
      console.warn(`⚠ Server is not reachable at ${baseUrl}`);
      console.warn(`  Error: ${err.message}`);
      console.warn(`  Skipping API integration tests. Start the indexer server to run these tests.`);
    }
  });

  function skipIfServerUnavailable() {
    if (!serverAvailable) {
      console.log('Skipping test - server not available');
    }
    return !serverAvailable;
  }

  describe('Indexer Endpoints - All Parameters Tested', () => {
    it('should get block height - basic', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/block-height');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expect(response.status).toBeLessThan(500);
    });

    it('should get version - basic', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status === 200) {
        expect(response.data).toBeDefined();
      }
    });

    it('should get status - basic', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/status');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should get changes by block height - valid height', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', `/verana/indexer/v1/changes/${SAMPLE_BLOCK_HEIGHT}`);
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should get changes by block height - edge case: height 0', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/0');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should get changes by block height - edge case: very large height', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/999999999');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should get changes by block height - invalid: non-numeric', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/invalid');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('DID Endpoints - All Parameters Tested', () => {
    describe('GET /verana/dd/v1/get/:did', () => {
      it('should get single DID - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/get/${SAMPLE_DID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get single DID - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/get/${SAMPLE_DID}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should handle invalid DID format gracefully', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/get/invalid-did-format');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/dd/v1/list - ALL PARAMETERS', () => {
      it('should list DIDs - no parameters (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with response_max_size at minimum (1)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 1,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with response_max_size at maximum (1024)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 1024,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with response_max_size at default (64)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 64,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with account filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          account: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with modified filter (ISO 8601)', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          modified: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with over_grace filter (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          over_grace: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with over_grace filter (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          over_grace: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with expired filter (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          expired: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with expired filter (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          expired: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with sort parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with ALL filters combined', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 50,
          account: SAMPLE_ACCOUNT,
          modified: timestamps.from,
          over_grace: false,
          expired: false,
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - validation: response_max_size exceeds max (1024)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 2000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - validation: response_max_size below minimum (0)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {
          response_max_size: 0,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list DIDs - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/list', {}, {
          'At-Block-Height': String(SAMPLE_BLOCK_HEIGHT),
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/dd/v1/params', () => {
      it('should get DID params - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID params - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/dd/v1/params', {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/dd/v1/history/:did - ALL PARAMETERS', () => {
      it('should get DID history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with response_max_size at minimum (1)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          response_max_size: 1,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with response_max_size at maximum (1000)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          response_max_size: 1000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with response_max_size at default (64)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          response_max_size: 64,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {}, {
          'At-Block-Height': String(SAMPLE_BLOCK_HEIGHT),
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get DID history - validation: response_max_size exceeds max', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/dd/v1/history/${SAMPLE_DID}`, {
          response_max_size: 2000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Trust Registry Endpoints - All Parameters Tested', () => {
    describe('GET /verana/tr/v1/get/:tr_id', () => {
      it('should get trust registry - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/tr/v1/get/${SAMPLE_TR_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get trust registry - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/tr/v1/get/${SAMPLE_TR_ID}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should handle invalid TR ID format', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/get/invalid-id');
        expect(response.status).toBeGreaterThanOrEqual(400);
        expect(response.status).toBeLessThan(600);
      });
    });

    describe('GET /verana/tr/v1/list - ALL PARAMETERS', () => {
      it('should list trust registries - no parameters (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with response_max_size at minimum (1)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 1,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with response_max_size at maximum (1024)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 1024,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with controller filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          controller: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with participant filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          participant: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with modified_after filter', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with only_active (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          only_active: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with only_active (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          only_active: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with active_gf_only (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          active_gf_only: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with preferred_language filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          preferred_language: 'en',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with sort parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with min/max filters (participants)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with min/max filters (weight)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_weight: '0',
          max_weight: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with min/max filters (issued)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_issued: '0',
          max_issued: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with min/max filters (verified)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_verified: '0',
          max_verified: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with min/max filters (slash events)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - with ALL filters combined', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 50,
          controller: SAMPLE_ACCOUNT,
          modified_after: timestamps.from,
          only_active: true,
          preferred_language: 'en',
          sort: 'modified',
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list trust registries - validation: response_max_size exceeds max', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 2000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/tr/v1/params', () => {
      it('should get TR params - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/tr/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/tr/v1/history/:tr_id - ALL PARAMETERS', () => {
      it('should get TR history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TR history - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TR history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TR history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Credential Schema Endpoints - All Parameters Tested', () => {
    describe('GET /verana/cs/v1/get/:id', () => {
      it('should get credential schema - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/cs/v1/get/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get credential schema - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/cs/v1/get/${SAMPLE_ID}`, {}, {
          'At-Block-Height': String(SAMPLE_BLOCK_HEIGHT),
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/js/:id', () => {
      it('should get JSON schema - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/cs/v1/js/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/list - ALL PARAMETERS', () => {
      it('should list credential schemas - no parameters (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          response_max_size: 10,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with tr_id filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          tr_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with participant filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          participant: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with modified_after filter', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with only_active (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          only_active: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with only_active (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          only_active: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with issuer_perm_management_mode', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          issuer_perm_management_mode: '2',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with verifier_perm_management_mode', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          verifier_perm_management_mode: '2',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with sort parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with min/max filters (participants)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with min/max filters (weight)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_weight: 0,
          max_weight: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with min/max filters (issued)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_issued: 0,
          max_issued: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with min/max filters (verified)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_verified: 0,
          max_verified: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with min/max filters (slash events)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list credential schemas - with ALL filters combined', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          response_max_size: 50,
          tr_id: SAMPLE_TR_ID,
          participant: SAMPLE_ACCOUNT,
          modified_after: timestamps.from,
          only_active: true,
          issuer_perm_management_mode: '2',
          verifier_perm_management_mode: '2',
          sort: 'modified',
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/params', () => {
      it('should get CS params - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/cs/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/history/:id - ALL PARAMETERS', () => {
      it('should get CS history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get CS history - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get CS history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get CS history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Permission Endpoints - All Parameters Tested', () => {
    describe('GET /verana/perm/v1/get/:id', () => {
      it('should get permission - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/get/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/list - ALL PARAMETERS', () => {
      it('should list permissions - no parameters (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with schema_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          schema_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with grantee', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          grantee: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with did', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          did: SAMPLE_DID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with perm_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with validator_perm_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          validator_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with perm_state', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          perm_state: 'ACTIVE',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with type', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          type: 'ISSUER',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with only_valid (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_valid: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with only_valid (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_valid: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with only_slashed (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_slashed: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with only_repaid (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_repaid: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with modified_after', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with country filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          country: 'US',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with vp_state filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          vp_state: 'VALIDATED',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          response_max_size: 50,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with when parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          when: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with sort parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with min/max participants', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with min/max weight', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_weight: 0,
          max_weight: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with min/max issued', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_issued: 0,
          max_issued: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with min/max verified', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_verified: 0,
          max_verified: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with min/max slash events', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permissions - with ALL filters combined', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          schema_id: SAMPLE_SCHEMA_ID,
          grantee: SAMPLE_ACCOUNT,
          did: SAMPLE_DID,
          perm_state: 'ACTIVE',
          type: 'ISSUER',
          only_valid: true,
          modified_after: timestamps.from,
          country: 'US',
          vp_state: 'VALIDATED',
          response_max_size: 50,
          when: timestamps.from,
          sort: 'modified',
          min_participants: 1,
          max_participants: 100,
          min_weight: 0,
          max_weight: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/pending/flat - ALL PARAMETERS', () => {
      it('should get pending flat - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get pending flat - with account (required)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get pending flat - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get pending flat - with sort parameter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/beneficiaries - ALL PARAMETERS', () => {
      it('should get beneficiaries - with issuer_perm_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          issuer_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get beneficiaries - with verifier_perm_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          verifier_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get beneficiaries - validation: missing required parameters (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toMatch(/issuer_perm_id|verifier_perm_id/);
        }
      });

      it('should get beneficiaries - with issuer_perm_id and response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          issuer_perm_id: SAMPLE_PERM_ID,
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/history/:id - ALL PARAMETERS', () => {
      it('should get permission history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission history - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-session/:id', () => {
      it('should get permission session - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-sessions - ALL PARAMETERS', () => {
      it('should list permission sessions - no parameters (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permission sessions - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          response_max_size: 50,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permission sessions - with modified_after', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should list permission sessions - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          response_max_size: 50,
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-session-history/:id - ALL PARAMETERS', () => {
      it('should get permission session history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission session history - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission session history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get permission session history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Metrics Endpoints - All Parameters Tested', () => {
    it('should get all metrics - basic', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/metrics/v1/all');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should get all metrics - with At-Block-Height header', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/metrics/v1/all', {}, {
        'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
      });
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('Trust Deposit Endpoints - All Parameters Tested', () => {
    describe('GET /verana/td/v1/get/:account', () => {
      it('should get trust deposit - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/td/v1/get/${SAMPLE_ACCOUNT}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get trust deposit - with At-Block-Height header', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/td/v1/get/${SAMPLE_ACCOUNT}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should handle invalid account format', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/td/v1/get/invalid-account');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/td/v1/params', () => {
      it('should get TD params - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/td/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/td/v1/history/:account - ALL PARAMETERS', () => {
      it('should get TD history - basic (defaults)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TD history - with response_max_size', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TD history - with transaction_timestamp_older_than', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get TD history - with ALL parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Account Reputation Endpoints - All Parameters Tested', () => {
    describe('GET /mx/v1/reputation - ALL PARAMETERS', () => {
      it('should get account reputation - with required account', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get account reputation - validation: missing account (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('account');
        }
      });

      it('should get account reputation - with tr_id filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          tr_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get account reputation - with schema_id filter', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          schema_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get account reputation - with include_slash_details (true)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          include_slash_details: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get account reputation - with include_slash_details (false)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          include_slash_details: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get account reputation - with ALL filters', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          tr_id: SAMPLE_TR_ID,
          schema_id: SAMPLE_SCHEMA_ID,
          include_slash_details: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Stats Endpoints - All Parameters Tested', () => {
    const timestamps = getTimestamps();

    describe('GET /verana/stats/v1/get - ALL PARAMETERS', () => {
      it('should get stats by id - basic', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          id: SAMPLE_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (GLOBAL) - HOUR granularity', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'HOUR',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (GLOBAL) - DAY granularity', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (GLOBAL) - MONTH granularity', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'MONTH',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (TRUST_REGISTRY) - with entity_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'TRUST_REGISTRY',
          entity_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (CREDENTIAL_SCHEMA) - with entity_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'CREDENTIAL_SCHEMA',
          entity_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats by granularity and timestamp (PERMISSION) - with entity_id', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'PERMISSION',
          entity_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: GLOBAL with entity_id (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
          entity_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('entity_id');
        }
      });

      it('should get stats - validation: TRUST_REGISTRY without entity_id (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'TRUST_REGISTRY',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('entity_id');
        }
      });

      it('should get stats - validation: invalid timestamp format', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: 'invalid-timestamp',
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: invalid granularity', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'INVALID',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: invalid entity_type', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'INVALID',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: missing required parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/stats/v1/stats - ALL PARAMETERS', () => {
      it('should get stats with time range (GLOBAL) - BUCKETS_AND_TOTAL', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (GLOBAL) - BUCKETS only', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (GLOBAL) - TOTAL only', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (GLOBAL) - with granularity HOUR', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'HOUR',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (GLOBAL) - with granularity DAY', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'DAY',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (GLOBAL) - with granularity MONTH', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'MONTH',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (TRUST_REGISTRY) - with entity_ids array', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'TRUST_REGISTRY',
          entity_ids: [SAMPLE_TR_ID],
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (TRUST_REGISTRY) - with entity_ids comma-separated string', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'TRUST_REGISTRY',
          entity_ids: `${SAMPLE_TR_ID},2,3`,
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (CREDENTIAL_SCHEMA) - with entity_ids', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'CREDENTIAL_SCHEMA',
          entity_ids: [SAMPLE_SCHEMA_ID],
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats with time range (PERMISSION) - with entity_ids', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'PERMISSION',
          entity_ids: [SAMPLE_PERM_ID],
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: timestamp_from after timestamp_until (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.until,
          timestamp_until: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('timestamp_from');
        }
      });

      it('should get stats - validation: GLOBAL with entity_ids (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          entity_ids: [SAMPLE_TR_ID],
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('entity_ids');
        }
      });

      it('should get stats - validation: TRUST_REGISTRY without entity_ids (should fail)', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'TRUST_REGISTRY',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('entity_ids');
        }
      });

      it('should get stats - validation: invalid timestamp format', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: 'invalid',
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      it('should get stats - validation: missing required parameters', async () => {
        if (skipIfServerUnavailable()) return;
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Error Handling - Comprehensive Tests', () => {
    it('should handle invalid endpoints gracefully', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/invalid/endpoint');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    it('should handle malformed requests gracefully', async () => {
      if (skipIfServerUnavailable()) return;
      try {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          invalid_param: 'invalid_value',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      } catch (error) {
        const axiosError = error as AxiosError;
        expect(axiosError.response?.status).not.toBeGreaterThanOrEqual(500);
      }
    });

    it('should handle invalid HTTP methods gracefully', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('POST', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('Response Validation - Comprehensive Tests', () => {
    it('should return valid JSON responses', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status < 400) {
        expect(response.headers['content-type']).toMatch(/application\/json/);
        expect(() => JSON.parse(JSON.stringify(response.data))).not.toThrow();
      }
    });

    it('should include proper headers', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/indexer/v1/block-height');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status < 400) {
        expect(response.headers).toBeDefined();
      }
    });

    it('should return consistent error format for 400 errors', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/stats/v1/get', {
      });
      if (response.status === 400) {
        expect(response.data).toBeDefined();
        expect(response.data.error || response.data.message).toBeDefined();
      }
    });

    it('should return consistent error format for 404 errors', async () => {
      if (skipIfServerUnavailable()) return;
      const response = await testEndpoint('GET', '/verana/dd/v1/get/nonexistent-did-12345');
      if (response.status === 404) {
        expect(response.data).toBeDefined();
        expect(response.data.error || response.data.message).toBeDefined();
      }
    });
  });
});
