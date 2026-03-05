import axios, { AxiosResponse } from "axios";
import { spawnSync } from "child_process";

const BASE_URL = "http://localhost:3001";
const TIMEOUT = 20000;

function serverReachableSync(baseUrl: string): boolean {
  const script = `
    const url = ${JSON.stringify(`${baseUrl}/verana/indexer/v1/version`)};
    const mod = url.startsWith("https://") ? require("https") : require("http");
    const req = mod.get(url, { timeout: 5000 }, (res) => {
      process.exit(res.statusCode && res.statusCode < 500 ? 0 : 2);
    });
    req.on("error", () => process.exit(1));
    req.on("timeout", () => { req.destroy(); process.exit(1); });
  `;
  const result = spawnSync(process.execPath, ["-e", script], {
    encoding: "utf8",
    windowsHide: true,
  });
  return result.status === 0;
}

const isAllowedEnv = ["development", "test"].includes((process.env.NODE_ENV || "").toLowerCase());
const canRun = isAllowedEnv && serverReachableSync(BASE_URL);
const describeIf: typeof describe = canRun ? describe : describe.skip;
const itIf: typeof it = canRun ? it : it.skip;

async function get(path: string, params?: Record<string, any>): Promise<AxiosResponse> {
  return axios.get(`${BASE_URL}${path}`, {
    params,
    timeout: TIMEOUT,
    validateStatus: () => true,
  });
}

describeIf("Participant role filters and openapi exposure", () => {
  itIf("perm/cs/tr list endpoints accept role filter and role sort params", async () => {
    const perm = await get("/verana/perm/v1/list", {
      response_max_size: 1,
      min_participants_ecosystem: 0,
      max_participants_ecosystem: 10,
      sort: "-participants_holder",
    });
    const cs = await get("/verana/cs/v1/list", {
      response_max_size: 1,
      min_participants_issuer: 0,
      max_participants_issuer: 10,
      sort: "-participants_verifier",
    });
    const tr = await get("/verana/tr/v1/list", {
      response_max_size: 1,
      min_participants_verifier_grantor: 0,
      max_participants_verifier_grantor: 10,
      sort: "-participants_issuer_grantor",
    });

    expect(perm.status).toBeLessThan(500);
    expect(cs.status).toBeLessThan(500);
    expect(tr.status).toBeLessThan(500);
  });

  itIf("global metrics includes role participant counters", async () => {
    const metrics = await get("/verana/mx/v1/all");
    expect(metrics.status).toBeLessThan(500);
    if (metrics.status === 200) {
      expect(metrics.data).toHaveProperty("participants_ecosystem");
      expect(metrics.data).toHaveProperty("participants_issuer_grantor");
      expect(metrics.data).toHaveProperty("participants_issuer");
      expect(metrics.data).toHaveProperty("participants_verifier_grantor");
      expect(metrics.data).toHaveProperty("participants_verifier");
      expect(metrics.data).toHaveProperty("participants_holder");
    }
  });

  itIf("openapi exposes new participant role filters", async () => {
    const spec = await get("/openapi.json");
    expect(spec.status).toBe(200);
    const text = JSON.stringify(spec.data);
    expect(text).toContain("min_participants_ecosystem");
    expect(text).toContain("max_participants_ecosystem");
    expect(text).toContain("min_participants_issuer_grantor");
    expect(text).toContain("max_participants_issuer_grantor");
    expect(text).toContain("min_participants_issuer");
    expect(text).toContain("max_participants_issuer");
    expect(text).toContain("min_participants_verifier_grantor");
    expect(text).toContain("max_participants_verifier_grantor");
    expect(text).toContain("min_participants_verifier");
    expect(text).toContain("max_participants_verifier");
    expect(text).toContain("min_participants_holder");
    expect(text).toContain("max_participants_holder");
  });
});
