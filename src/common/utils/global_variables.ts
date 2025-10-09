import ModuleParams from "../../models/modules_params";

export default async function getGlobalVariables() {
  try {
    const modules = await ModuleParams.query();

    if (!modules || modules.length === 0) {
      return {};
    }

    const globalVariables: Record<string, any> = {};

    for (const module of modules) {
      if (!module || !module.params) continue;

      const parsedParams =
        typeof module.params === "string"
          ? JSON.parse(module.params)
          : module.params;

      globalVariables[module.module] = parsedParams.params || parsedParams;
    }

    return globalVariables;
  } catch (err: any) {
    console.error("Error fetching global variables", err);
    return {};
  }
}
