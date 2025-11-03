/* eslint-disable @typescript-eslint/no-var-requires */
import { Service } from "@ourparentcenter/moleculer-decorators-extended";
import { IncomingMessage, ServerResponse } from "http";
import { Context, ServiceBroker } from "moleculer";
import OpenApiMixin from "moleculer-auto-openapi";
import ApiGateway, { Route } from "moleculer-web";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import { swaggerUiComponent } from "./swagger_ui";

async function attachHeaders(res: ServerResponse) {
  try {
    const checkpoint = await knex("block_checkpoint")
      .where("job_name", "crawl:block")
      .first();

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
          await attachHeaders(res);
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
          await attachHeaders(res);
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
          await attachHeaders(res);
          return data;
        },
      },
      {
        path: "/verana/perm/v1",
        aliases: {
          "GET get/:id": `${SERVICE.V1.PermAPIService.path}.getPermission`,
          "GET list": `${SERVICE.V1.PermAPIService.path}.listPermissions`,
          "GET beneficiaries": `${SERVICE.V1.PermAPIService.path}.findBeneficiaries`,
          "GET permission-session/:id": `${SERVICE.V1.PermAPIService.path}.getPermissionSession`,
          "GET permission-sessions": `${SERVICE.V1.PermAPIService.path}.listPermissionSessions`,
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
          await attachHeaders(res);
          return data;
        },
      },
      {
        path: "/verana/td/v1",
        aliases: {
          "GET get/:account": `${SERVICE.V1.TrustDepositApiService.path}.getTrustDeposit`,
          "GET params": `${SERVICE.V1.TrustDepositApiService.path}.getModuleParams`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        }, onAfterCall: async function (
          _ctx: Context<any, any>,
          _route: Route,
          _req: IncomingMessage,
          res: ServerResponse,
          data: any
        ) {
          await attachHeaders(res);
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
