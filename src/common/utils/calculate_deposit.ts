import { readGenesis } from "./genesis_reader";

export async function calculateDidDeposit(years: number = 1): Promise<number> {
  try {
    if (years < 1 || years > 31) {
      throw new Error("Years must be between 1 and 31");
    }
    const didParams = await readGenesis("app_state.diddirectory.params");
    if (!didParams || didParams.length === 0) {
      throw new Error("Failed to read did_directory_trust_deposit from genesis.json");
    }
    const didDirectoryTrustDeposit = Number(didParams[0]?.did_directory_trust_deposit);
    if (Number.isNaN(didDirectoryTrustDeposit)) {
      throw new Error("did_directory_trust_deposit is not a valid number");
    }

    const globalParams = await readGenesis("app_state.trustregistry.params");
    if (!globalParams || globalParams.length === 0) {
      throw new Error("Failed to read trust_unit_price from genesis.json");
    }
    const trustUnitPrice = Number(globalParams[0]?.trust_unit_price);
    if (Number.isNaN(trustUnitPrice)) {
      throw new Error("trust_unit_price is not a valid number");
    }

    const didDeposit = trustUnitPrice * didDirectoryTrustDeposit * years;

    return didDeposit;
  } catch (error) {
    console.error("Error calculating DID deposit:", error);
    throw error;
  }
}
