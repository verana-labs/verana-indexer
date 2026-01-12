export function computeParamsChanges(
  oldParams: any,
  newParams: any,
  relevantFields?: string[]
): Record<string, { old: any; new: any }> | null {
  if (!oldParams || !newParams) return null;

  const changes: Record<string, { old: any; new: any }> = {};

  const keysToCheck = relevantFields || Object.keys(oldParams).filter(key => key in newParams);

  for (const key of keysToCheck) {
    const oldValue = oldParams[key];
    const newValue = newParams[key];

    const normalizeValue = (val: any) => {
      if (val === null || val === undefined) return null;
      if (typeof val === 'string') return val.trim();
      return val;
    };

    const normalizedOld = normalizeValue(oldValue);
    const normalizedNew = normalizeValue(newValue);

    if (String(normalizedOld) !== String(normalizedNew)) {
      changes[key] = { old: normalizedOld, new: normalizedNew };
    }
  }

  return Object.keys(changes).length ? changes : null;
}

export function hasMeaningfulChanges(oldParams: any, newParams: any): boolean {
  if (!oldParams && newParams && Object.keys(newParams).length > 0) return true; 
  if (oldParams && !newParams) return true; 
  if (!oldParams || !newParams) return false;

  const commonKeys = Object.keys(oldParams).filter(key => key in newParams);
  for (const key of commonKeys) {
    const oldValue = oldParams[key];
    const newValue = newParams[key];

    const normalizeValue = (val: any) => {
      if (val === null || val === undefined) return null;
      if (typeof val === 'string') return val.trim();
      return val;
    };

    const normalizedOld = normalizeValue(oldValue);
    const normalizedNew = normalizeValue(newValue);

    if (String(normalizedOld) !== String(normalizedNew)) {
      return true;
    }
  }

  const newKeys = Object.keys(newParams).filter(key => !(key in oldParams));
  if (newKeys.length > 0) {
    return true; 
  }

  return false;
}

export async function recordModuleParamsHistorySafe(
  trx: any,
  module: string,
  params: any,
  eventType: string,
  height: number,
  previousParams?: any,
  relevantFields?: string[]
) {
  let shouldRecord = false;
  let changes = null;

  if (eventType === "CREATE_PARAMS") {
    shouldRecord = true;
    changes = computeParamsChanges(null, params, relevantFields) || {};
  } else if (eventType === "UPDATE_PARAMS") {
    changes = computeParamsChanges(previousParams, params, relevantFields);
    shouldRecord = !!changes;
  }

  if (shouldRecord) {
    const existingRecord = await trx("module_params_history")
      .where({ module, height, event_type: eventType })
      .first();

    if (!existingRecord) {
      await trx("module_params_history").insert({
        module,
        params: JSON.stringify(params),
        event_type: eventType,
        height,
        changes: changes ? JSON.stringify(changes) : null,
      });
    }
  }
}
