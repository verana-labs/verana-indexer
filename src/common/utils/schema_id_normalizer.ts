import { Network } from "../../network";

const DEFAULT_CHAIN_ID = "vna-testnet-1";

const ID_PATTERN = /("\$id"\s*:\s*)"[^"]*"/;

export function overrideSchemaIdInString(schemaString: string, actualId: number): string {
  if (typeof schemaString !== "string") {
    return schemaString;
  }
  const chainId = Network.chainId || DEFAULT_CHAIN_ID;
  const canonicalId = `vpr:verana:${chainId}/cs/v1/js/${actualId}`;
  if (ID_PATTERN.test(schemaString)) {
    return schemaString.replace(ID_PATTERN, `$1"${canonicalId}"`);
  }
  const afterBraceMatch = schemaString.match(/^\{\s*(\n?)/);
  if (afterBraceMatch && afterBraceMatch[1]) {
    const indentMatch = schemaString.match(/^\{\s*\n(\s*)/);
    const indent = indentMatch && indentMatch[1] ? indentMatch[1] : "  ";
    return schemaString.replace(/^\{\s*\n/, `{\n${indent}"$id": "${canonicalId}",\n`);
  }
  return schemaString.replace(/^(\{\s*)/, `$1"$id": "${canonicalId}", `);
}
