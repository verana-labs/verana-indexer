import { Context } from "moleculer";

export function getBlockHeight(ctx: Context<any, any>): number | undefined {
  return (ctx.meta as any)?.blockHeight;
}

export function hasBlockHeight(ctx: Context<any, any>): boolean {
  const blockHeight = getBlockHeight(ctx);
  return typeof blockHeight === "number";
}

