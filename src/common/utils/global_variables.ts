import ModuleParams from "../../models/modules_params";
import { parseModuleParams } from "./params_service";

export default async function getGlobalVariables() {
  try {
    const modules = await ModuleParams.query();

    if (!modules || modules.length === 0) {
      return {};
    }

    const globalVariables: Record<string, any> = {};

    for (const module of modules) {
      if (!module || !module.params) continue;
      globalVariables[module.module] = parseModuleParams(module.params);
    }

    return globalVariables;
  } catch (err: any) {
    console.error("Error fetching global variables", err);
    return {};
  }
}
