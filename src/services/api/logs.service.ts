import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import fs from "fs";
import path from "path";
import BaseService from "../../base/base.service";

@Service({
  name: "LogsService",
  version: 1,
})
export default class LogsService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

 
 
 
  @Action({
    name: "downloadErrors",
    params: {},
  })
  public async downloadErrors(_ctx: Context) {
    const LOG_DIR = process.env.LOG_DIR || path.join(process.cwd(), "logs");
    const ERR_FILE = process.env.ERROR_LOG_FILE || "errors.log";
    const hasPathInFile = ERR_FILE.includes("/") || ERR_FILE.includes("\\");
    const filePath = path.isAbsolute(ERR_FILE)
      ? ERR_FILE
      : hasPathInFile
        ? path.resolve(process.cwd(), ERR_FILE)
        : path.join(LOG_DIR, ERR_FILE);
    try {
      if (!fs.existsSync(filePath)) {
        return { file: filePath, content: "" };
      }
      const content = fs.readFileSync(filePath, "utf8");
      return { file: filePath, content };
    } catch (err: any) {
      this.logger?.error("Error reading errors file for download:", err);
      throw err;
    }
  }
}

