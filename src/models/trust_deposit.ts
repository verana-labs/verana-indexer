import BaseModel from "./base";

export default class TrustDeposit extends BaseModel {
  static tableName = "trust_deposits";

  id!: number;
  corporation!: string;
  share!: number;
  deposit!: number;
  claimable!: number;
  slashed_deposit!: number;
  repaid_deposit!: number;
  last_slashed!: string | null;
  last_repaid!: string | null;
  slash_count!: number;

  static get jsonSchema() {
    return {
      type: "object",
      required: ["corporation", "share", "deposit"],
      properties: {
        id: { type: "integer" },
        corporation: { type: "string", maxLength: 255 },
        share: { type: "number" },
        deposit: { type: "number" },
        claimable: { type: "number" },
        slashed_deposit: { type: "number" },
        repaid_deposit: { type: "number" },
        last_slashed: { type: ["string", "null"] },
        last_repaid: { type: ["string", "null"] },
        slash_count: { type: "integer" },
      },
    };
  }

  getAvailableAmount(): number {
    const total = this.deposit ?? 0;
    const slashed = this.slashed_deposit ?? 0;
    const repaid = this.repaid_deposit ?? 0;
    return total - slashed + repaid;
  }
}
