import BaseModel from "./base";

export default class TrustDeposit extends BaseModel {
  static tableName = "trust_deposits";

  id!: number;
  account!: string;
  share!: string;
  amount!: string;
  augend!: string;
  claimable!: string;
  slashed_deposit!: string;
  repaid_deposit!: string;
  last_slashed!: string | null;
  last_repaid!: string | null;
  slash_count!: string;
  last_repaid_by!: string;

  static get jsonSchema() {
    return {
      type: "object",
      required: ["account", "share", "amount"],
      properties: {
        id: { type: "integer" },
        account: { type: "string", maxLength: 255 },
        share: { type: "string" },
        amount: { type: "string" },
        claimable: { type: "string" },
        slashed_deposit: { type: "string" },
        repaid_deposit: { type: "string" },
        last_slashed: { type: ["string", "null"] },
        last_repaid: { type: ["string", "null"] },
        slash_count: { type: "string" },
        last_repaid_by: { type: "string" },
      },
    };
  }

  getAvailableAmount(): string {
    const total = BigInt(this.amount);
    const slashed = BigInt(this.slashed_deposit || "0");
    const repaid = BigInt(this.repaid_deposit || "0");
    return (total - slashed + repaid).toString();
  }
}
