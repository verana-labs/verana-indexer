/* eslint-disable @typescript-eslint/no-var-requires */
import { Service } from "@ourparentcenter/moleculer-decorators-extended";
import { IncomingMessage, ServerResponse } from "http";
import { Context, ServiceBroker, Errors } from "moleculer";
import OpenApiMixin from "moleculer-auto-openapi";
import ApiGateway, { Route } from "moleculer-web";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import { swaggerUiComponent } from "./swagger_ui";

const BLOCK_CHECKPOINT_JOB = "crawl:block";

async function fetchBlockCheckpoint() {
  return knex("block_checkpoint")
    .where("job_name", BLOCK_CHECKPOINT_JOB)
    .first();
}

async function ensureAtBlockHeight(
  ctx: Context<any, any>,
  req: IncomingMessage
) {
  ctx.meta = ctx.meta || {};
  const headerValue =
    (req.headers.atblockheight ??
      req.headers["at-blockheight"] ??
      req.headers["at-block-height"]) ??
    null;

  if (headerValue === null) {
    throw new Errors.MoleculerError(
      "Missing AtBlockHeight header",
      428,
      "AT_BLOCK_HEIGHT_REQUIRED"
    );
  }

  const parsedHeight = Number(headerValue);
  if (!Number.isInteger(parsedHeight) || parsedHeight < 0) {
    throw new Errors.MoleculerError(
      "AtBlockHeight must be a positive integer",
      400,
      "AT_BLOCK_HEIGHT_INVALID"
    );
  }

  let checkpoint = ctx.meta.latestCheckpoint;
  if (!checkpoint) {
    checkpoint = await fetchBlockCheckpoint();
    if (!checkpoint) {
      throw new Errors.MoleculerError(
        "Indexer checkpoint unavailable",
        503,
        "AT_BLOCK_HEIGHT_UNAVAILABLE"
      );
    }
    ctx.meta.latestCheckpoint = checkpoint;
  }

  if (parsedHeight > checkpoint.height) {
    throw new Errors.MoleculerError(
      `Requested height ${parsedHeight} exceeds indexed height ${checkpoint.height}`,
      409,
      "AT_BLOCK_HEIGHT_AHEAD"
    );
  }

  ctx.meta.blockHeight = parsedHeight;
}

async function attachHeaders(ctx: Context<any, any>, res: ServerResponse) {
  try {
    let checkpoint = ctx?.meta?.latestCheckpoint;
    if (!checkpoint) {
      checkpoint = await fetchBlockCheckpoint();
    }

    if (checkpoint) {
      res.setHeader(
        "X-Index-Ts",
        checkpoint.updated_at?.toISOString?.() ?? checkpoint.updated_at
      );
      res.setHeader("X-Height", checkpoint.height.toString());
    }
  } catch (err) {
    console.log(err);
  }

  res.setHeader("X-Query-At", new Date().toISOString());
}


const ensureBlockHeightForIndexer = async function (
  ctx: Context<any, any>,
  _route: Route,
  req: IncomingMessage
) {
  if (req.url?.includes("changes")) {
    await ensureAtBlockHeight(ctx, req);
  }
};

@Service({
  name: "api",
  mixins: [ApiGateway, OpenApiMixin],
  settings: {
    port: process.env.PORT || 3001,

    routes: [
      {
        path: "/verana/dd/v1",
        aliases: {
          "GET get/:did": `${SERVICE.V1.DidDatabaseService.path}.getSingleDid`,
          "GET list": `${SERVICE.V1.DidDatabaseService.path}.getDidList`,
          "GET history/:did": `${SERVICE.V1.DidHistoryService.path}.getByDid`,
          "GET params": `${SERVICE.V1.DidDatabaseService.path}.getDidParams`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/verana/cs/v1",
        aliases: {
          "GET get/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.get`,
          "GET history/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.getHistory`,
          "GET js/:id": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.JsonSchema`,
          "GET list": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.list`,
          "GET params": `${SERVICE.V1.CredentialSchemaDatabaseService.path}.getParams`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },

        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/verana/tr/v1",
        aliases: {
          "GET get/:tr_id": `${SERVICE.V1.TrustRegistryDatabaseService.path}.getTrustRegistry`,
          "GET list": `${SERVICE.V1.TrustRegistryDatabaseService.path}.listTrustRegistries`,
          "GET params": `${SERVICE.V1.TrustRegistryDatabaseService.path}.getParams`,
          "GET history/:tr_id": `${SERVICE.V1.TrustRegistryHistoryService.path}.getTRHistory`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/verana/perm/v1",
        aliases: {
          "GET get/:id": `${SERVICE.V1.PermAPIService.path}.getPermission`,
          "GET list": `${SERVICE.V1.PermAPIService.path}.listPermissions`,
          "GET beneficiaries": `${SERVICE.V1.PermAPIService.path}.findBeneficiaries`,
          "GET history/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionHistory`,
          "GET permission-session/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionSession`,
          "GET permission-sessions": `${SERVICE.V1.PermAPIService.path}.listPermissionSessions`,
          "GET permission-session-history/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionSessionHistory`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/verana/td/v1",
        aliases: {
          "GET get/:account": `${SERVICE.V1.TrustDepositApiService.path}.getTrustDeposit`,
          "GET params": `${SERVICE.V1.TrustDepositApiService.path}.getModuleParams`,
          "GET history/:account": `${SERVICE.V1.TrustDepositApiService.path}.getTrustDepositHistory`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/mx/v1",
        aliases: {
          "GET reputation": `${SERVICE.V1.AccountReputationService.path}.getAccountReputation`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
      {
        path: "/verana/indexer/v1",
        aliases: {
          "GET block-height": `${SERVICE.V1.IndexerMetaService.path}.getBlockHeight`,
          "GET changes": `${SERVICE.V1.IndexerMetaService.path}.listChanges`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
        onBeforeCall: ensureBlockHeightForIndexer,
        onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(_ctx, res);
          return data;
        },
      },
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
}

