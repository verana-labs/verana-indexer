import * as dotenv from "dotenv";
import { loadEnvFiles } from "./common/utils/loadEnv";

loadEnvFiles();
dotenv.config();

export const Network = {
  chainId: process.env.CHAIN_ID || "vna-testnet-1",
  RPC: process.env.RPC_ENDPOINT,
  LCD: process.env.LCD_ENDPOINT,
  databaseName: process.env.POSTGRES_DB || "verana_testnet1",
  redisDBNumber: Number(process.env.REDIS_DB_NUMBER) || 20,
  moleculerNamespace: process.env.MOLECULER_NAMESPACE || "Verana-Testnet",
  EVMJSONRPC: [process.env.EVM_JSON_RPC],
  EVMchainId: Number(process.env.EVM_CHAIN_ID) || 26657,
};
