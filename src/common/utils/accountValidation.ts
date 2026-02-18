const ACCOUNT_REGEX = /^verana1[0-9a-z]{10,}$/;

export function isValidAccount(account: string): boolean {
  return ACCOUNT_REGEX.test(account);
}

export function validateParticipantParam(
  value: unknown,
  paramName: string = "participant"
): { valid: true; value: string | undefined } | { valid: false; error: string } {
  const provided = value !== undefined && value !== null && value !== "";
  if (!provided) {
    return { valid: true, value: undefined };
  }
  const normalized = typeof value === "string" ? value.trim() : String(value).trim();
  if (normalized === "" || /^\d+$/.test(normalized)) {
    return {
      valid: false,
      error: `Invalid ${paramName} parameter: expected a valid account address (non-empty, not purely numeric). Got: ${JSON.stringify(value)}`,
    };
  }
  return { valid: true, value: normalized };
}


export function validateRequiredAccountParam(
  value: unknown,
  paramName: string = "account"
): { valid: true; value: string } | { valid: false; error: string } {
  if (value === undefined || value === null) {
    return { valid: false, error: `Missing required parameter: ${paramName}` };
  }
  const normalized = typeof value === "string" ? value.trim() : String(value).trim();
  if (normalized === "") {
    return { valid: false, error: `Invalid ${paramName} parameter: expected a non-empty value. Got: ${JSON.stringify(value)}` };
  }
  if (/^\d+$/.test(normalized)) {
    return {
      valid: false,
      error: `Invalid ${paramName} parameter: expected a valid account address (not purely numeric). Got: ${JSON.stringify(value)}`,
    };
  }
  return { valid: true, value: normalized };
}

