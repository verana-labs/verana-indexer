import { Context } from "moleculer";

interface Meta {
  $statusCode?: number;
}

export default class ApiResponder {
  static success<T>(ctx: Context<any, Meta>, data: T, status = 200) {
    ctx.meta.$statusCode = status;
    return data;
  }

  static error(ctx: Context<any, Meta>, message: string, code = 500) {
    ctx.meta.$statusCode = code;
    return { error: message, code };
  }
}
