/* eslint-disable @typescript-eslint/no-var-requires */
import { Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import ApiGateway from "moleculer-web";
import OpenApiMixin from "moleculer-auto-openapi";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";

@Service({
  name: "api",
  mixins: [ApiGateway, OpenApiMixin],
  settings: {
    port: process.env.PORT || 3000,
    routes: [
      {
        path: "/verana/dd/v1",
        aliases: {
          "GET get/:did": `${SERVICE.V1.DidDatabaseService.path}.getSingleDid`,
          "GET list": `${SERVICE.V1.DidDatabaseService.path}.getDidList`,
          "GET history/:did": `${SERVICE.V1.DidHistoryService.path}.getByDid`,
        },
        mappingPolicy: "restrict",
        bodyParsers: {
          json: true,
          urlencoded: { extended: true },
        },
      },
    ],
    openapi: {
      info: {
        title: "Verana DID API",
        version: "1.0.0",
        description: "Auto-generated OpenAPI docs for Verana DID APIs",
      },
      servers: [
        { url: "http://localhost:3000/verana/dd/v1", description: "Local Dev" },
      ],
    },
  },
})
export default class ApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }
}
