import { Network } from '../../network';

export function extractSchemaIdFromNormalizedId(normalizedId: string): number | null {
  if (!normalizedId || typeof normalizedId !== 'string') {
    return null;
  }
  
  const match = normalizedId.match(/\/js\/(\d+)$/);
  if (match && match[1]) {
    const id = parseInt(match[1], 10);
    return Number.isNaN(id) ? null : id;
  }
  
  return null;
}

export function normalizeSchemaId(
  jsonSchema: any,
  schemaId: number
): any {
  if (!jsonSchema) {
    return jsonSchema;
  }

  let schemaObj: any;

  if (typeof jsonSchema === 'string') {
    try {
      schemaObj = JSON.parse(jsonSchema);
    } catch {
      return jsonSchema;
    }
  } else {
    schemaObj = jsonSchema;
  }

  if (!schemaObj || typeof schemaObj !== 'object') {
    return jsonSchema;
  }

  const chainId = Network.chainId || "vna-testnet-1";
  const canonicalId = `vpr:verana:${chainId}/cs/v1/js/${schemaId}`;

  const normalizedSchema = {
    ...schemaObj,
    $id: canonicalId,
  };

  if (typeof jsonSchema === 'string') {
    return JSON.stringify(normalizedSchema);
  }

  return normalizedSchema;
}

export function needsIdNormalization(jsonSchema: any, schemaId: number): boolean {
  if (!jsonSchema) {
    return false;
  }

  let schemaObj: any;
  if (typeof jsonSchema === 'string') {
    try {
      schemaObj = JSON.parse(jsonSchema);
    } catch {
      return false;
    }
  } else {
    schemaObj = jsonSchema;
  }

  if (!schemaObj || typeof schemaObj !== 'object') {
    return false;
  }

  const chainId = Network.chainId || "vna-testnet-1";
  const expectedId = `vpr:verana:${chainId}/cs/v1/js/${schemaId}`;

  if (!schemaObj.$id) {
    return true;
  }

  return schemaObj.$id !== expectedId;
}
