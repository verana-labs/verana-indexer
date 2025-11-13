import Knex from "knex";
import { knexConfig } from "../../knexfile";

const environment = process.env.NODE_ENV || 'development';
const cfg = knexConfig[environment];

if (!cfg) {
  throw new Error(`Knex configuration not found for environment: ${environment}`);
}

const knex = Knex(cfg);
export default knex;
