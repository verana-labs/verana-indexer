import BaseModel from "./base";

export default class ExchangeRate extends BaseModel {
  static tableName = "exchange_rates";

  id!: number;
  base_asset_type!: string;
  base_asset!: string;
  quote_asset_type!: string;
  quote_asset!: string;
  rate!: string;
  rate_scale!: number;
  validity_duration!: number;
  updated!: string | null;
  expires!: string | null;
  state!: boolean;

  static get jsonSchema() {
    return {
      type: "object",
      required: [
        "id",
        "base_asset_type",
        "base_asset",
        "quote_asset_type",
        "quote_asset",
        "rate",
        "rate_scale",
        "state",
      ],
      properties: {
        id: { type: "integer" },
        base_asset_type: { type: "string" },
        base_asset: { type: "string", maxLength: 255 },
        quote_asset_type: { type: "string" },
        quote_asset: { type: "string", maxLength: 255 },
        rate: { type: "string" },
        rate_scale: { type: "integer" },
        validity_duration: { type: "integer" },
        updated: { type: ["string", "null"] },
        expires: { type: ["string", "null"] },
        state: { type: "boolean" },
      },
    };
  }
}
