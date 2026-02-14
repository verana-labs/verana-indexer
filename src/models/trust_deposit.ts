import BaseModel from "./base";

export default class TrustDeposit extends BaseModel {
  static tableName = "trust_deposits";

  id!: number;
  account!: string;
  share!: number;
  amount!: number;
  claimable!: number;
  slashed_deposit!: number;
  repaid_deposit!: number;
  last_slashed!: string | null;
  last_repaid!: string | null;
  slash_count!: number;
  last_repaid_by!: string;

  static get jsonSchema() {
    return {
      type: "object",
      required: ["account", "share", "amount"],
      properties: {
        id: { type: "integer" },
        account: { type: "string", maxLength: 255 },
        share: { type: "number" },
        amount: { type: "number" },
        claimable: { type: "number" },
        slashed_deposit: { type: "number" },
        repaid_deposit: { type: "number" },
        last_slashed: { type: ["string", "null"] },
        last_repaid: { type: ["string", "null"] },
        slash_count: { type: "integer" },
        last_repaid_by: { type: "string" },
      },
    };
  }

  getAvailableAmount(): number {
    const total = this.amount ?? 0;
    const slashed = this.slashed_deposit ?? 0;
    const repaid = this.repaid_deposit ?? 0;
    return total - slashed + repaid;
  }
}
