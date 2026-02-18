import { Network } from "../../network";

const DEFAULT_CHAIN_ID = "vna-testnet-1";

const ID_PATTERN = /("\$id"\s*:\s*)"[^"]*"/;

export function overrideSchemaIdInString(schemaString: string, actualId: number): string {
  if (typeof schemaString !== "string") {
    return schemaString;
  }
  const chainId = Network.chainId || DEFAULT_CHAIN_ID;
  const canonicalId = `vpr:verana:${chainId}/cs/v1/js/${actualId}`;
  return schemaString.replace(ID_PATTERN, `$1"${canonicalId}"`);
}
