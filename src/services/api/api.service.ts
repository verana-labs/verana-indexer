/* eslint-disable @typescript-eslint/no-var-requires */
import { Service } from "@ourparentcenter/moleculer-decorators-extended";
import { IncomingMessage, Server, ServerResponse } from "http";
import { Context, ServiceBroker, Errors } from "moleculer";
import OpenApiMixin from "moleculer-auto-openapi";
import ApiGateway, { Route } from "moleculer-web";
import BaseService from "../../base/base.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import { swaggerUiComponent } from "./swagger_ui";
import { eventsBroadcaster } from "./events_broadcaster";
import { indexerStatusManager } from "../manager/indexer_status.manager";

const BLOCK_CHECKPOINT_JOB = BULL_JOB_NAME.HANDLE_TRANSACTION;
const REQUEST_ACCEPTED_NS = Symbol("requestAcceptedNs");
const REQUEST_START_NS = Symbol("requestStartNs");
const requestTimingHookedServers = new WeakSet<Server>();


const DEFAULT_ROUTE_CONFIG = {
  mappingPolicy: "restrict" as const,
  bodyParsers: {
    json: true,
    urlencoded: { extended: true },
  },
};

async function fetchBlockCheckpoint() {
  return knex("block_checkpoint")
    .select("height", "updated_at")
    .where("job_name", BLOCK_CHECKPOINT_JOB)
    .first();
}

function getHeaderValue(req: IncomingMessage): string | null {
  const normalizedHeader = "at-block-height";
  let value = req.headers[normalizedHeader];
  if (Array.isArray(value)) {
    value = value[0];
  }
  if (value !== undefined && value !== null) {
    const strValue = String(value).trim();
    return strValue || null;
  }
  const headerKey = Object.keys(req.headers).find(
    key => key.toLowerCase() === normalizedHeader
  );
  if (headerKey) {
    let fallbackValue = req.headers[headerKey];
    if (Array.isArray(fallbackValue)) {
      fallbackValue = fallbackValue[0];
    }
    if (fallbackValue !== undefined && fallbackValue !== null) {
      const strValue = String(fallbackValue).trim();
      return strValue || null;
    }
  }

  return null;
}

function attachIndexerStatusHeaders(res: ServerResponse) {
  const status = indexerStatusManager.getStatus();
  res.setHeader("X-Indexer-Status", status.isRunning ? "running" : "stopped");
  res.setHeader("X-Crawling-Status", status.isCrawling ? "active" : "stopped");

  if (!status.isRunning || !status.isCrawling) {
    const errorMessage = status.lastError?.message || status.stoppedReason || "";
    const isUnknown = isUnknownMessageError(errorMessage);

    if (isUnknown) {
      if (status.stoppedReason) {
        res.setHeader("X-Crawling-Reason", status.stoppedReason);
      }
      if (status.lastError?.message) {
        res.setHeader("X-Crawling-Error", status.lastError.message);
      }
    }
    if (status.stoppedAt) {
      res.setHeader("X-Crawling-Stopped-At", status.stoppedAt);
    }
  }
}

async function parseAtBlockHeight(
  ctx: Context<any, any>,
  req: IncomingMessage,
  required: boolean = false
) {
  ctx.meta = ctx.meta || {};
  const headerValue = getHeaderValue(req);

  if (!headerValue) {
    if (required) {
      throw new Errors.MoleculerError(
        "Missing At-Block-Height header",
        400,
        "AT_BLOCK_HEIGHT_REQUIRED"
      );
    }
    return;
  }

  const parsedHeight = Number(headerValue);
  if (!Number.isInteger(parsedHeight) || parsedHeight < 0) {
    throw new Errors.MoleculerError(
      "At-Block-Height must be a positive integer",
      400,
      "AT_BLOCK_HEIGHT_INVALID"
    );
  }

  let checkpoint = ctx.meta.latestCheckpoint;
  if (!checkpoint) {
    checkpoint = await fetchBlockCheckpoint();
    if (!checkpoint) {
     checkpoint = { height: 0 } as any;
    }
    ctx.meta.latestCheckpoint = checkpoint;
  }

  if (checkpoint && checkpoint.height > 0 && parsedHeight > checkpoint.height) {
    throw new Errors.MoleculerError(
      `Requested height ${parsedHeight} exceeds indexed height ${checkpoint.height}`,
      409,
      "AT_BLOCK_HEIGHT_AHEAD"
    );
  }

  ctx.meta.blockHeight = parsedHeight;
  ctx.meta.$headers = ctx.meta.$headers || {};
  ctx.meta.$headers["at-block-height"] = String(parsedHeight);
  ctx.meta.$headers["At-Block-Height"] = String(parsedHeight);
}

function isUnknownMessageError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  return errorMessage.includes('Unknown Verana message types') ||
         errorMessage.includes('UNKNOWN VERANA MESSAGE TYPES');
}

async function attachHeaders(ctx: Context<any, any>, res: ServerResponse) {
  try {
    let checkpoint = ctx?.meta?.latestCheckpoint;
    if (!checkpoint) {
      checkpoint = await fetchBlockCheckpoint();
      if (!checkpoint) {
        checkpoint = { height: 0, updated_at: null } as { height: number; updated_at?: Date | string | null };
      }
    }

    if (checkpoint) {
      let indexTs: string;
      if (checkpoint.updated_at) {
        const updatedAt =
          checkpoint.updated_at instanceof Date
            ? checkpoint.updated_at
            : new Date(checkpoint.updated_at);
        const iso = updatedAt.toISOString();
        indexTs = iso.replace(/\.\d{3}Z$/, "Z");
      } else {
        indexTs = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
      }
      res.setHeader("X-Index-Ts", indexTs);
      res.setHeader("X-Height", checkpoint.height.toString());
    } else if (typeof ctx?.meta?.blockHeight === "number") {
      res.setHeader("X-Height", String(ctx.meta.blockHeight));
    } else {
      res.setHeader("X-Height", "0");
    }

    attachIndexerStatusHeaders(res);
  } catch (err: any) {
    res.setHeader("X-Height", "0");
    attachIndexerStatusHeaders(res);
  }

  res.setHeader("X-Query-At", new Date().toISOString());
}

function setResponseTimeHeader(req: IncomingMessage, res: ServerResponse) {
  const acceptedNs = (req as any)[REQUEST_ACCEPTED_NS];
  const startNs = (req as any)[REQUEST_START_NS];
  if (typeof startNs === "bigint") {
    const nowNs = process.hrtime.bigint();
    let responseMs = Number(nowNs - startNs) / 1_000_000;
    if (typeof acceptedNs === "bigint") {
      responseMs = Number(nowNs - acceptedNs) / 1_000_000;
    }
    res.setHeader("X-Response-Time-Ms", responseMs.toFixed(2));
  }
}

function attachRequestTimingHook(server: Server) {
  if (requestTimingHookedServers.has(server)) {
    return;
  }
  server.prependListener("request", (req: IncomingMessage) => {
    if (typeof (req as any)[REQUEST_ACCEPTED_NS] !== "bigint") {
      (req as any)[REQUEST_ACCEPTED_NS] = process.hrtime.bigint();
    }
  });
  requestTimingHookedServers.add(server);
}

function createOnBeforeCall(required: boolean = true) {
  return async function (
    ctx: Context<any, any>,
    _route: Route,
    req: IncomingMessage
  ) {
    try {
      (req as any)[REQUEST_START_NS] = process.hrtime.bigint();
      const urlPath = req.url || '';
      const isStatusEndpoint = urlPath.includes('/verana/indexer/v1/status') || 
                               urlPath.endsWith('/status') ||
                               (ctx.action?.name || '').includes('IndexerStatusService.getStatus');
      
      if (isStatusEndpoint) {
        const status = indexerStatusManager.getStatus();
        if (!status.isRunning) {
          const error = new Errors.MoleculerError(
            `Indexer is not responding. ${status.stoppedReason || 'Indexer stopped.'} ${status.lastError ? `Error: ${status.lastError.message}` : ''}`,
            503,
            "INDEXER_STOPPED"
          );
          throw error;
        }
      }
      await parseAtBlockHeight(ctx, req, required);
    } catch (err: any) {
      const error = err instanceof Errors.MoleculerError
        ? err
        : new Errors.MoleculerError(
          err?.message || "Internal error",
          err?.code || 500,
          err?.type || "INTERNAL_ERROR"
        );
      throw error;
    }
  };
}

function createOnError() {
  return async function (
    req: IncomingMessage,
    res: ServerResponse,
    err: any
  ) {
    try {
      const status = err.code || err.status || 500;
      const errorMessage = err.message || err.error || "Internal Server Error";
      const errorType = err.type || err.name || "UNKNOWN_ERROR";

      const indexerStatus = indexerStatusManager.getStatus();

      let heightValue = "0";
      try {
        const checkpoint = await fetchBlockCheckpoint();
        if (checkpoint?.height != null) heightValue = String(checkpoint.height);
      } catch (_) {}
      res.setHeader("X-Height", heightValue);

      res.setHeader("X-Indexer-Status", indexerStatus.isRunning ? "running" : "stopped");
      res.setHeader("X-Crawling-Status", indexerStatus.isCrawling ? "active" : "stopped");
      
      const isUnknown = isUnknownMessageError(errorMessage);
      if (isUnknown) {
        res.setHeader("X-Error-Type", errorType);
        res.setHeader("X-Error-Message", errorMessage);
      }

      if (!indexerStatus.isCrawling) {
        const statusError = indexerStatus.lastError?.message || indexerStatus.stoppedReason || '';
        const isStatusUnknown = isUnknownMessageError(statusError);
        
        if (isStatusUnknown) {
          if (indexerStatus.stoppedReason) {
            res.setHeader("X-Crawling-Reason", indexerStatus.stoppedReason);
          }
          if (indexerStatus.lastError?.message) {
            res.setHeader("X-Crawling-Error", indexerStatus.lastError.message);
          }
        }
        if (indexerStatus.stoppedAt) {
          res.setHeader("X-Crawling-Stopped-At", indexerStatus.stoppedAt);
        }
      }

      res.setHeader("X-Query-At", new Date().toISOString());
      setResponseTimeHeader(req, res);

      if (!res.headersSent) {
        res.writeHead(status, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: errorMessage, code: status }));
      }
    } catch (handlerError: any) {
      console.error("Error in onError handler:", handlerError);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Internal Server Error", code: 500 }));
      }
    }
  };
}

function createOnAfterCall() {
  return async function (
    _ctx: Context<any, any>,
    _route: Route,
    req: IncomingMessage,
    res: ServerResponse,
    data: any
  ) {
    await attachHeaders(_ctx, res);
    setResponseTimeHeader(req, res);
    if ((_ctx.meta as any).$rawJsonResponse) {
      let rawData: string;
      if (typeof data === "string") {
        if (data.startsWith('"') && data.endsWith('"') && data.length > 1) {
          rawData = JSON.parse(data);
        } else {
          rawData = data;
        }
      } else {
        rawData = JSON.stringify(data);
      }

      const statusCode = (_ctx.meta as any).$statusCode || 200;
      if (!res.headersSent) {
        res.writeHead(statusCode, { "Content-Type": "application/json" });
      } else {
        res.setHeader("Content-Type", "application/json");
      }
      res.end(rawData);
      return undefined;
    }
    return data;
  };
}

function createRoute(
  path: string,
  aliases: Record<string, string>,
  requireBlockHeight: boolean = false
) {
  return {
    path,
    aliases,
    ...DEFAULT_ROUTE_CONFIG,
    onBeforeCall: createOnBeforeCall(requireBlockHeight),
    onError: createOnError(),
    onAfterCall: createOnAfterCall(),
  };
}

@Service({
  name: "api",
  mixins: [ApiGateway, OpenApiMixin],
  settings: {
    port: process.env.PORT || 3001,

    routes: [
      createRoute("/verana/dd/v1", {
        "GET get/:did": `${SERVICE.V1.DidDatabaseService.path}.getSingleDid`,
        "GET list": `${SERVICE.V1.DidDatabaseService.path}.getDidList`,
        "GET history/:did": `${SERVICE.V1.DidHistoryService.path}.getByDid`,
        "GET params": `${SERVICE.V1.DidDatabaseService.path}.getDidParams`,
      }),
      createRoute("/verana/cs/v1", {
        "GET get/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.get`,
        "GET history/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.getHistory`,
        "GET js/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.JsonSchema`,
        "GET list": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.list`,
        "GET params": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.getParams`,
      }),
      createRoute("/verana/tr/v1", {
        "GET get/:tr_id": `${SERVICE.V1.TrustRegistryDatabaseService.path}.getTrustRegistry`,
        "GET list": `${SERVICE.V1.TrustRegistryDatabaseService.path}.listTrustRegistries`,
        "GET params": `${SERVICE.V1.TrustRegistryDatabaseService.path}.getParams`,
        "GET history/:tr_id": `${SERVICE.V1.TrustRegistryHistoryService.path}.getTRHistory`,
      }),
      createRoute("/verana/perm/v1", {
        "GET get/:id": `${SERVICE.V1.PermAPIService.path}.getPermission`,
        "GET list": `${SERVICE.V1.PermAPIService.path}.listPermissions`,
        "GET pending/flat": `${SERVICE.V1.PermAPIService.path}.pendingFlat`,
        "GET beneficiaries": `${SERVICE.V1.PermAPIService.path}.findBeneficiaries`,
        "GET history/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionHistory`,
        "GET permission-session/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionSession`,
        "GET permission-sessions": `${SERVICE.V1.PermAPIService.path}.listPermissionSessions`,
        "GET permission-session-history/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionSessionHistory`,
      }),
      createRoute("/verana/metrics/v1", {
        "GET all": `${SERVICE.V1.MetricsApiService.path}.getAll`,
      }),
      createRoute("/verana/td/v1", {
        "GET get/:account": `${SERVICE.V1.TrustDepositApiService.path}.getTrustDeposit`,
        "GET params": `${SERVICE.V1.TrustDepositApiService.path}.getModuleParams`,
        "GET history/:account": `${SERVICE.V1.TrustDepositApiService.path}.getTrustDepositHistory`,
      }),
      createRoute("/mx/v1", {
        "GET reputation": `${SERVICE.V1.AccountReputationService.path}.getAccountReputation`,
      }),
      createRoute("/verana/indexer/v1", {
        "GET block-height": `${SERVICE.V1.IndexerMetaService.path}.getBlockHeight`,
        "GET changes/:block_height": `${SERVICE.V1.IndexerMetaService.path}.listChanges`,
        "GET version": `${SERVICE.V1.IndexerMetaService.path}.getVersion`,
        "GET status": `${SERVICE.V1.IndexerStatusService.path}.getDetailedStatus`,
        "GET errors/download": `v1.LogsService.downloadErrors`,
      }),
      createRoute("/verana/stats/v1", {
        "GET get": `${SERVICE.V1.StatsAPIService.path}.get`,
        "GET stats": `${SERVICE.V1.StatsAPIService.path}.stats`,
        "GET participants-at-height": `${SERVICE.V1.StatsAPIService.path}.getParticipantsAtHeight`,
      }), 
      {
        path: "/",
        ...swaggerUiComponent(),
      },
    ],
  },
})
export default class ApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  async started() {
    await new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve();
      }, 1000);
    });

    const server = (this as unknown as { server?: Server }).server;
    if (server) {
      attachRequestTimingHook(server);
      eventsBroadcaster.setLogger(this.logger);
      eventsBroadcaster.initialize(server);
      this.logger.info("✅ WebSocket events broadcaster initialized on /verana/indexer/v1/events");
    } else {
      this.logger.warn("⚠️ HTTP server not found, WebSocket events broadcaster not initialized");
    }
  }

  async stopped() {
    eventsBroadcaster.close();
    this.logger.info("🔌 WebSocket events broadcaster closed");
  }
}
