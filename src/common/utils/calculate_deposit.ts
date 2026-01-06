import fs from "fs";
import path from "path";
import ModuleParams from "../../models/modules_params";
import { ModulesParamsNamesTypes } from "../constant";

export async function calculateDidDeposit(years: number = 1): Promise<number> {
  if (years < 1 || years > 31) {
    throw new Error("Years must be between 1 and 31");
  }

  let didDirectoryTrustDeposit: number | null = null;
  let trustUnitPrice: number | null = null;

  const didModule = await ModuleParams.query().findOne({
    module: ModulesParamsNamesTypes?.DD,
  });
  if (didModule?.params) {
    const parsed =
      typeof didModule.params === "string"
        ? JSON.parse(didModule.params)
        : didModule.params;
    if (parsed?.params?.did_directory_trust_deposit != null) {
      didDirectoryTrustDeposit = Number(
        parsed.params.did_directory_trust_deposit
      );
    }
  }

  const trustModule = await ModuleParams.query().findOne({
    module: ModulesParamsNamesTypes?.TR,
  });
  if (trustModule?.params) {
    const parsed =
      typeof trustModule.params === "string"
        ? JSON.parse(trustModule.params)
        : trustModule.params;
    if (parsed?.params?.trust_unit_price != null) {
      trustUnitPrice = Number(parsed.params.trust_unit_price);
    }
  }

  if (didDirectoryTrustDeposit === null || trustUnitPrice === null) {
    const genesisPath = path.resolve("genesis.json");
    
    let retries = 3;
    let waitTime = 1000;
    
    while (retries > 0 && (!fs.existsSync(genesisPath) || (didDirectoryTrustDeposit === null || trustUnitPrice === null))) {
      if (fs.existsSync(genesisPath)) {
        try {
          const raw = fs.readFileSync(genesisPath, "utf-8");
          const genesis = JSON.parse(raw);
          const appState = genesis.app_state || {};

          if (
            didDirectoryTrustDeposit === null &&
            appState.diddirectory?.params?.did_directory_trust_deposit != null
          ) {
            didDirectoryTrustDeposit = Number(
              appState.diddirectory.params.did_directory_trust_deposit
            );
          }

          if (
            trustUnitPrice === null &&
            appState.trustregistry?.params?.trust_unit_price != null
          ) {
            trustUnitPrice = Number(appState.trustregistry.params.trust_unit_price);
          }
          
          if (didDirectoryTrustDeposit !== null && trustUnitPrice !== null) {
            break;
          }
        } catch (error) {
          const err = error as Error;
          if (!err.message) {
            throw err;
          }
        }
      }
      
      if (didDirectoryTrustDeposit === null || trustUnitPrice === null) {
        retries--;
        if (retries > 0) {
          const currentWaitTime = waitTime;
          await new Promise<void>((resolve) => {
            setTimeout(() => {
              resolve();
            }, currentWaitTime);
          });
          waitTime *= 2;
        }
      }
    }
  }

  if (didDirectoryTrustDeposit === null || trustUnitPrice === null) {
    throw new Error("Unable to determine DID deposit parameters");
  }

  const didDeposit = didDirectoryTrustDeposit * trustUnitPrice * years;
  return didDeposit;
}
