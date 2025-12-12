const ACCOUNT_REGEX = /^verana1[0-9a-z]{10,}$/;

export function isValidAccount(account: string): boolean {
  return ACCOUNT_REGEX.test(account);
}

