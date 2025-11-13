/* eslint-disable import/no-import-module-exports */

import { createBullBoard } from "@bull-board/api";
import { BullAdapter } from "@bull-board/api/bullAdapter";
import { ExpressAdapter } from "@bull-board/express";
import Queue from "bull";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import * as redisuri from "redisuri";
import { Network } from "../../network";
import { DEFAULT_PREFIX } from "../../base/bullable.service";
import { BULL_JOB_NAME, Config } from "../../common";

export const bullBoardMixin = () => ({
  async started() {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.logger.info("🚀 Generating Bull Board");

    const redisUriComponent = redisuri.parse(Config.QUEUE_JOB_REDIS);
    let rootRedisURI: string;

    if (redisUriComponent.host && redisUriComponent.port) {
      if (redisUriComponent.auth) {
        rootRedisURI = `redis://${redisUriComponent.auth}@${redisUriComponent.host}:${redisUriComponent.port}`;
      } else {
        rootRedisURI = `redis://${redisUriComponent.host}:${redisUriComponent.port}`;
      }
    } else {
      throw Error("❌ BULL REDIS URI is invalid");
    }

    // ✅ Use single Network object directly
    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath(`/admin/queues/${Network.chainId}`);

    const { setQueues } = createBullBoard({
      queues: [],
      serverAdapter,
    });

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.addRoute({
      path: `/admin/queues/${Network.chainId}`,
      use: [serverAdapter.getRouter()],
    });

    const listQueues = Object.values({
      ...BULL_JOB_NAME,
    }).map(
      (queueName) =>
        new BullAdapter(
          Queue(queueName, `${rootRedisURI}/${Network.redisDBNumber}`, {
            prefix: DEFAULT_PREFIX,
          })
        )
    );

    setQueues(listQueues);
  },
});
