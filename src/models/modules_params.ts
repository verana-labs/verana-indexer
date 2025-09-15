import BaseModel from "./base";

export default class ModuleParams extends BaseModel {
  id!: number;
  module!: string;
  params!: object;
  created_at!: Date;
  updated_at!: Date;

  static tableName = "module_params";

  static jsonSchema = {
    type: "object",
    required: ["module", "params"],
    properties: {
      id: { type: "integer" },
      module: { type: "string", minLength: 1, maxLength: 255 },
      params: { type: "object" },
      created_at: { type: "string", format: "date-time" },
      updated_at: { type: "string", format: "date-time" },
    },
  };
}
