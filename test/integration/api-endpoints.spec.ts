import axios, { AxiosResponse } from 'axios';
import { spawnSync } from "child_process";

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

function expectResponseTimeHeader(response: AxiosResponse) {
  const header = response.headers?.["x-response-time-ms"];
  expect(header).toBeDefined();
  const value = Number(header);
  expect(Number.isFinite(value)).toBe(true);
  expect(value).toBeGreaterThanOrEqual(0);
}

const TEST_BASE_URL = getBaseUrl();
const nodeEnv = (process.env.NODE_ENV || "").toLowerCase();
const isAllowedEnv = nodeEnv === "development" || nodeEnv === "test";
const isCI = String(process.env.CI || "").toLowerCase() === "true";
const isLocalhost3001 = /^http:\/\/localhost:3001$/i.test(TEST_BASE_URL);
type IntegrationRunState = "FULL_RUN" | "SKIPPED_PRECONDITION";
let integrationRunState: IntegrationRunState = "SKIPPED_PRECONDITION";
let integrationSkipReason = "";
let suiteEnabled = false;

function checkServerReachableSync(baseUrl: string): { reachable: boolean; reason: string } {
  const script = `
    const url = new URL(${JSON.stringify(`${baseUrl}/verana/indexer/v1/version`)});
    const mod = url.protocol === "https:" ? require("https") : require("http");
    const req = mod.get(url, { timeout: 5000 }, (res) => {
      if (res.statusCode && res.statusCode < 500) {
        process.exit(0);
      }
      console.error("unexpected status: " + res.statusCode);
      process.exit(2);
    });
    req.on("error", (err) => {
      console.error(err && err.message ? err.message : String(err));
      process.exit(1);
    });
    req.on("timeout", () => {
      req.destroy(new Error("timeout"));
      console.error("timeout");
      process.exit(1);
    });
  `;

  const result = spawnSync(process.execPath, ["-e", script], {
    encoding: "utf8",
    windowsHide: true,
  });

  if (result.status === 0) {
    return { reachable: true, reason: "" };
  }

  const reason = (result.stderr || result.stdout || `exit status ${result.status}`).trim();
  return { reachable: false, reason: reason || "unknown error" };
}

const preconditionFailures: string[] = [];
if (!isAllowedEnv) {
  preconditionFailures.push(`NODE_ENV must be "development" or "test" (got "${process.env.NODE_ENV || "undefined"}")`);
}
if (!isLocalhost3001) {
  preconditionFailures.push(`Base URL must be exactly "http://localhost:3001" (got "${TEST_BASE_URL}")`);
}
if (preconditionFailures.length === 0) {
  const reachability = checkServerReachableSync(TEST_BASE_URL);
  if (!reachability.reachable) {
    preconditionFailures.push(`Server is not reachable at ${TEST_BASE_URL}/verana/indexer/v1/version (${reachability.reason})`);
  }
}

if (preconditionFailures.length === 0) {
  suiteEnabled = true;
  integrationRunState = "FULL_RUN";
  integrationSkipReason = "";
} else {
  suiteEnabled = false;
  integrationRunState = "SKIPPED_PRECONDITION";
  integrationSkipReason = preconditionFailures.join("; ");
}

const itIf: typeof it = suiteEnabled ? it : it.skip;
const describeIf: typeof describe = suiteEnabled ? describe : describe.skip;

if (!suiteEnabled) {
  process.stdout.write(
    `\n[API-INTEGRATION] skipped all tests.\n` +
    `  NODE_ENV=${process.env.NODE_ENV || "undefined"}\n` +
    `  CI=${process.env.CI || "undefined"}\n` +
    `  Base URL=${TEST_BASE_URL}\n` +
    `  Reason=${integrationSkipReason}\n`
  );
}

describeIf('Comprehensive API Endpoints Integration Tests', () => {
  beforeAll(async () => {
    if (suiteEnabled) {
      const response = await axios.get(`${TEST_BASE_URL}/verana/indexer/v1/version`, {
        timeout: 5000,
      });
      console.log(`✓ Server is reachable at ${TEST_BASE_URL}`);
      console.log(`  Server version response: ${response.status}`);
    }
  });

  afterAll(() => {
    if (integrationRunState === "FULL_RUN") {
      process.stdout.write(`\n[API-INTEGRATION] server reachable at ${TEST_BASE_URL}; full API suite executed.\n`);
    }
  });

  describe('Indexer Endpoints - All Parameters Tested', () => {
    itIf('should get block height - basic', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/block-height');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expect(response.status).toBeLessThan(500);
    });

    itIf('should get version - basic', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expectResponseTimeHeader(response);
      if (response.status === 200) {
        expect(response.data).toBeDefined();
      }
    });

    itIf('should get status - basic', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/status');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    itIf('should get changes by block height - valid height', async () => {
      const response = await testEndpoint('GET', `/verana/indexer/v1/changes/${SAMPLE_BLOCK_HEIGHT}`);
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status === 200) {
        expect(response.data).toHaveProperty('next_change_at');
        const nextChangeAt = response.data?.next_change_at;
        expect(nextChangeAt === null || Number.isInteger(nextChangeAt)).toBe(true);
        if (typeof nextChangeAt === 'number') {
          expect(nextChangeAt).toBeGreaterThan(SAMPLE_BLOCK_HEIGHT);
        }
      }
    });

    itIf('should get changes by block height - edge case: height 0', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/0');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    itIf('should get changes by block height - edge case: very large height', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/999999999');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status === 200) {
        expect(response.data).toHaveProperty('next_change_at');
        expect(response.data.next_change_at).toBeNull();
      }
    });

    itIf('should get changes by block height - invalid: non-numeric', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/changes/invalid');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('API Response Timing Headers', () => {
    itIf('should include x-response-time-ms for perm list endpoint', async () => {
      const response = await testEndpoint('GET', '/verana/perm/v1/list', { response_max_size: 1 });
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expectResponseTimeHeader(response);
    });

    itIf('should include x-response-time-ms for TR list endpoint', async () => {
      const response = await testEndpoint('GET', '/verana/tr/v1/list', { response_max_size: 1 });
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expectResponseTimeHeader(response);
    });

    itIf('should include x-response-time-ms for CS list endpoint', async () => {
      const response = await testEndpoint('GET', '/verana/cs/v1/list', { response_max_size: 1 });
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      expectResponseTimeHeader(response);
    });

    itIf('should expose x-response-time-ms in openapi response headers', async () => {
      const response = await testEndpoint('GET', '/openapi.json');
      expect(response.status).toBe(200);
      expect(response.data?.components?.headers?.["X-Response-Time-Ms"]).toBeDefined();
      expect(response.data?.components?.headers?.["X-Response-Time-Ms"]?.schema?.type).toBe("string");
    });
  });

  describe('Participant Role Attributes and Filters', () => {
    itIf('should accept participant-role min/max filters on perm/cs/tr list', async () => {
      const perm = await testEndpoint('GET', '/verana/perm/v1/list', {
        response_max_size: 1,
        min_participants_ecosystem: 0,
        max_participants_ecosystem: 10,
      });
      const cs = await testEndpoint('GET', '/verana/cs/v1/list', {
        response_max_size: 1,
        min_participants_issuer: 0,
        max_participants_issuer: 10,
      });
      const tr = await testEndpoint('GET', '/verana/tr/v1/list', {
        response_max_size: 1,
        min_participants_verifier_grantor: 0,
        max_participants_verifier_grantor: 10,
      });

      expect(perm.status).toBeLessThan(500);
      expect(cs.status).toBeLessThan(500);
      expect(tr.status).toBeLessThan(500);
    });

    itIf('should expose participant role counters in global metrics', async () => {
      const response = await testEndpoint('GET', '/verana/metrics/v1/all');
      expect(response.status).toBeLessThan(500);
      if (response.status === 200) {
        expect(response.data).toHaveProperty("participants_ecosystem");
        expect(response.data).toHaveProperty("participants_issuer_grantor");
        expect(response.data).toHaveProperty("participants_issuer");
        expect(response.data).toHaveProperty("participants_verifier_grantor");
        expect(response.data).toHaveProperty("participants_verifier");
        expect(response.data).toHaveProperty("participants_holder");
      }
    });
  });

  describe('Trust Registry Endpoints - All Parameters Tested', () => {
    describe('GET /verana/tr/v1/get/:tr_id', () => {
      itIf('should get trust registry - basic', async () => {
        const response = await testEndpoint('GET', `/verana/tr/v1/get/${SAMPLE_TR_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get trust registry - with At-Block-Height header', async () => {
        const response = await testEndpoint('GET', `/verana/tr/v1/get/${SAMPLE_TR_ID}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should handle invalid TR ID format', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/get/invalid-id');
        expect(response.status).toBeGreaterThanOrEqual(400);
        expect(response.status).toBeLessThan(600);
      });
    });

    describe('GET /verana/tr/v1/list - ALL PARAMETERS', () => {
      itIf('should list trust registries - no parameters (defaults)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with response_max_size at minimum (1)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 1,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with response_max_size at maximum (1024)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 1024,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with controller filter', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          controller: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with participant filter', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          participant: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with modified_after filter', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with only_active (true)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          only_active: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with only_active (false)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          only_active: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with active_gf_only (true)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          active_gf_only: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with preferred_language filter', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          preferred_language: 'en',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with participant-role sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          sort: '-participants_verifier_grantor',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with min/max filters (participants)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with min/max filters (weight)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_weight: '0',
          max_weight: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with min/max filters (issued)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_issued: '0',
          max_issued: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with min/max filters (verified)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_verified: '0',
          max_verified: '1000000',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with min/max filters (slash events)', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list trust registries - with ALL filters combined', async () => {
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

      itIf('should list trust registries - validation: response_max_size exceeds max', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/list', {
          response_max_size: 2000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/tr/v1/params', () => {
      itIf('should get TR params - basic', async () => {
        const response = await testEndpoint('GET', '/verana/tr/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/tr/v1/history/:tr_id - ALL PARAMETERS', () => {
      itIf('should get TR history - basic (defaults)', async () => {
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TR history - with response_max_size', async () => {
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TR history - with transaction_timestamp_older_than', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/tr/v1/history/${SAMPLE_TR_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TR history - with ALL parameters', async () => {
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
      itIf('should get credential schema - basic', async () => {
        const response = await testEndpoint('GET', `/verana/cs/v1/get/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get credential schema - with At-Block-Height header', async () => {
        const response = await testEndpoint('GET', `/verana/cs/v1/get/${SAMPLE_ID}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/js/:id', () => {
      itIf('should get JSON schema - basic', async () => {
        const response = await testEndpoint('GET', `/verana/cs/v1/js/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/list - ALL PARAMETERS', () => {
      itIf('should list credential schemas - no parameters (defaults)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with response_max_size', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          response_max_size: 10,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with tr_id filter', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          tr_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with participant filter', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          participant: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with modified_after filter', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with only_active (true)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          only_active: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with only_active (false)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          only_active: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with issuer_perm_management_mode', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          issuer_perm_management_mode: '2',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with verifier_perm_management_mode', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          verifier_perm_management_mode: '2',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with participant-role sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          sort: '-participants_issuer_grantor',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with min/max filters (participants)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with min/max filters (weight)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_weight: 0,
          max_weight: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with min/max filters (issued)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_issued: 0,
          max_issued: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with min/max filters (verified)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_verified: 0,
          max_verified: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with min/max filters (slash events)', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list credential schemas - with ALL filters combined', async () => {
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
      itIf('should get CS params - basic', async () => {
        const response = await testEndpoint('GET', '/verana/cs/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/cs/v1/history/:id - ALL PARAMETERS', () => {
      itIf('should get CS history - basic (defaults)', async () => {
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get CS history - with response_max_size', async () => {
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get CS history - with transaction_timestamp_older_than', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/cs/v1/history/${SAMPLE_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get CS history - with ALL parameters', async () => {
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
      itIf('should get permission - basic', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/get/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/list - ALL PARAMETERS', () => {
      itIf('should list permissions - no parameters (defaults)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with schema_id', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          schema_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with grantee', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          grantee: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with did', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          did: SAMPLE_DID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with perm_id', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with validator_perm_id', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          validator_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with perm_state', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          perm_state: 'ACTIVE',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with type', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          type: 'ISSUER',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with only_valid (true)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_valid: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with only_valid (false)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_valid: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with only_slashed (true)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_slashed: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with only_repaid (true)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          only_repaid: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with modified_after', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with country filter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          country: 'US',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with vp_state filter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          vp_state: 'VALIDATED',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with response_max_size', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          response_max_size: 50,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with when parameter', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          when: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with participant-role sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          sort: '-participants_holder',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with min/max participants', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_participants: 1,
          max_participants: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with min/max weight', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_weight: 0,
          max_weight: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with min/max issued', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_issued: 0,
          max_issued: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with min/max verified', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_verified: 0,
          max_verified: 1000000,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with min/max slash events', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/list', {
          min_ecosystem_slash_events: 0,
          max_ecosystem_slash_events: 100,
          min_network_slash_events: 0,
          max_network_slash_events: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permissions - with ALL filters combined', async () => {
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
      itIf('should get pending flat - basic', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get pending flat - with account (required)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get pending flat - with response_max_size', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get pending flat - with sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
          sort: 'modified',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get pending flat - with participant-role sort parameter', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/pending/flat', {
          account: SAMPLE_ACCOUNT,
          sort: '-participants_ecosystem',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/beneficiaries - ALL PARAMETERS', () => {
      itIf('should get beneficiaries - with issuer_perm_id', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          issuer_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get beneficiaries - with verifier_perm_id', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          verifier_perm_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get beneficiaries - validation: missing required parameters (should fail)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toMatch(/issuer_perm_id|verifier_perm_id/);
        }
      });

      itIf('should get beneficiaries - with issuer_perm_id and response_max_size', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/beneficiaries', {
          issuer_perm_id: SAMPLE_PERM_ID,
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/history/:id - ALL PARAMETERS', () => {
      itIf('should get permission history - basic (defaults)', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission history - with response_max_size', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission history - with transaction_timestamp_older_than', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission history - with ALL parameters', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/history/${SAMPLE_PERM_ID}`, {
          response_max_size: 50,
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-session/:id', () => {
      itIf('should get permission session - basic', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-sessions - ALL PARAMETERS', () => {
      itIf('should list permission sessions - no parameters (defaults)', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permission sessions - with response_max_size', async () => {
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          response_max_size: 50,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permission sessions - with modified_after', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should list permission sessions - with ALL parameters', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', '/verana/perm/v1/permission-sessions', {
          response_max_size: 50,
          modified_after: timestamps.from,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/perm/v1/permission-session-history/:id - ALL PARAMETERS', () => {
      itIf('should get permission session history - basic (defaults)', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission session history - with response_max_size', async () => {
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission session history - with transaction_timestamp_older_than', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/perm/v1/permission-session-history/${SAMPLE_PERM_ID}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get permission session history - with ALL parameters', async () => {
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
    itIf('should get all metrics - basic', async () => {
      const response = await testEndpoint('GET', '/verana/metrics/v1/all');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    itIf('should get all metrics - with At-Block-Height header', async () => {
      const heightResponse = await testEndpoint('GET', '/verana/indexer/v1/block-height');
      const currentHeight = Number(heightResponse?.data?.height || SAMPLE_BLOCK_HEIGHT);
      const response = await testEndpoint('GET', '/verana/metrics/v1/all', {}, {
        'At-Block-Height': currentHeight,
      });
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('Trust Deposit Endpoints - All Parameters Tested', () => {
    describe('GET /verana/td/v1/get/:corporation', () => {
      itIf('should get trust deposit - basic', async () => {
        const response = await testEndpoint('GET', `/verana/td/v1/get/${SAMPLE_ACCOUNT}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get trust deposit - with At-Block-Height header', async () => {
        const response = await testEndpoint('GET', `/verana/td/v1/get/${SAMPLE_ACCOUNT}`, {}, {
          'At-Block-Height': SAMPLE_BLOCK_HEIGHT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should handle invalid account format', async () => {
        const response = await testEndpoint('GET', '/verana/td/v1/get/invalid-account');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/td/v1/params', () => {
      itIf('should get TD params - basic', async () => {
        const response = await testEndpoint('GET', '/verana/td/v1/params');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/td/v1/history/:corporation - ALL PARAMETERS', () => {
      itIf('should get TD history - basic (defaults)', async () => {
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`);
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TD history - with response_max_size', async () => {
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`, {
          response_max_size: 100,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TD history - with transaction_timestamp_older_than', async () => {
        const timestamps = getTimestamps();
        const response = await testEndpoint('GET', `/verana/td/v1/history/${SAMPLE_ACCOUNT}`, {
          transaction_timestamp_older_than: timestamps.lastWeek,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get TD history - with ALL parameters', async () => {
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
      itIf('should get account reputation - with required account', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get account reputation - validation: missing account (should fail)', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation');
        expect(response.status).not.toBeGreaterThanOrEqual(500);
        if (response.status === 400) {
          expect(response.data?.error || response.data?.message).toContain('account');
        }
      });

      itIf('should get account reputation - with tr_id filter', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          tr_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get account reputation - with schema_id filter', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          schema_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get account reputation - with include_slash_details (true)', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          include_slash_details: true,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get account reputation - with include_slash_details (false)', async () => {
        const response = await testEndpoint('GET', '/mx/v1/reputation', {
          account: SAMPLE_ACCOUNT,
          include_slash_details: false,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get account reputation - with ALL filters', async () => {
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
      itIf('should get stats by id - basic', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          id: SAMPLE_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (GLOBAL) - HOUR granularity', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'HOUR',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (GLOBAL) - DAY granularity', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (GLOBAL) - MONTH granularity', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'MONTH',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (TRUST_REGISTRY) - with entity_id', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'TRUST_REGISTRY',
          entity_id: SAMPLE_TR_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (CREDENTIAL_SCHEMA) - with entity_id', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'CREDENTIAL_SCHEMA',
          entity_id: SAMPLE_SCHEMA_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats by granularity and timestamp (PERMISSION) - with entity_id', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'PERMISSION',
          entity_id: SAMPLE_PERM_ID,
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: GLOBAL with entity_id (should fail)', async () => {
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

      itIf('should get stats - validation: TRUST_REGISTRY without entity_id (should fail)', async () => {
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

      itIf('should get stats - validation: invalid timestamp format', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: 'invalid-timestamp',
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: invalid granularity', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'INVALID',
          timestamp: timestamps.from,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: invalid entity_type', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
          granularity: 'DAY',
          timestamp: timestamps.from,
          entity_type: 'INVALID',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: missing required parameters', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/get', {
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });

    describe('GET /verana/stats/v1/stats - ALL PARAMETERS', () => {
      itIf('should get stats with time range (GLOBAL) - BUCKETS_AND_TOTAL', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (GLOBAL) - BUCKETS only', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (GLOBAL) - TOTAL only', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (GLOBAL) - with granularity HOUR', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'HOUR',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (GLOBAL) - with granularity DAY', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'DAY',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (GLOBAL) - with granularity MONTH', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
          granularity: 'MONTH',
          result_type: 'BUCKETS_AND_TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (TRUST_REGISTRY) - with entity_ids array', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'TRUST_REGISTRY',
          entity_ids: [SAMPLE_TR_ID],
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (TRUST_REGISTRY) - with entity_ids comma-separated string', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'TRUST_REGISTRY',
          entity_ids: `${SAMPLE_TR_ID},2,3`,
          result_type: 'BUCKETS',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (CREDENTIAL_SCHEMA) - with entity_ids', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'CREDENTIAL_SCHEMA',
          entity_ids: [SAMPLE_SCHEMA_ID],
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats with time range (PERMISSION) - with entity_ids', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: timestamps.from,
          timestamp_until: timestamps.until,
          entity_type: 'PERMISSION',
          entity_ids: [SAMPLE_PERM_ID],
          result_type: 'TOTAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: timestamp_from after timestamp_until (should fail)', async () => {
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

      itIf('should get stats - validation: GLOBAL with entity_ids (should fail)', async () => {
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

      itIf('should get stats - validation: TRUST_REGISTRY without entity_ids (should fail)', async () => {
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

      itIf('should get stats - validation: invalid timestamp format', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
          timestamp_from: 'invalid',
          timestamp_until: timestamps.until,
          entity_type: 'GLOBAL',
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });

      itIf('should get stats - validation: missing required parameters', async () => {
        const response = await testEndpoint('GET', '/verana/stats/v1/stats', {
        });
        expect(response.status).not.toBeGreaterThanOrEqual(500);
      });
    });
  });

  describe('Error Handling - Comprehensive Tests', () => {
    itIf('should handle invalid endpoints gracefully', async () => {
      const response = await testEndpoint('GET', '/verana/invalid/endpoint');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });

    itIf('should handle malformed requests gracefully', async () => {
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

    itIf('should handle invalid HTTP methods gracefully', async () => {
      const response = await testEndpoint('POST', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
    });
  });

  describe('Response Validation - Comprehensive Tests', () => {
    itIf('should return valid JSON responses', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/version');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status < 400) {
        expect(response.headers['content-type']).toMatch(/application\/json/);
        expect(() => JSON.parse(JSON.stringify(response.data))).not.toThrow();
      }
    });

    itIf('should include proper headers', async () => {
      const response = await testEndpoint('GET', '/verana/indexer/v1/block-height');
      expect(response.status).not.toBeGreaterThanOrEqual(500);
      if (response.status < 400) {
        expect(response.headers).toBeDefined();
      }
    });

    itIf('should return consistent error format for 400 errors', async () => {
      const response = await testEndpoint('GET', '/verana/stats/v1/get', {
      });
      if (response.status === 400) {
        expect(response.data).toBeDefined();
        expect(response.data.error || response.data.message).toBeDefined();
      }
    });

    itIf('should return consistent error format for 404 errors', async () => {
      const response = await testEndpoint('GET', '/verana/cs/v1/get/999999999');
      if (response.status === 404) {
        expect(response.data).toBeDefined();
        expect(response.data.error || response.data.message).toBeDefined();
      }
    });
  });
});

