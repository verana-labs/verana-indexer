/* eslint-disable import/no-extraneous-dependencies */
import { Model, AjvValidator } from 'objection';
import addFormats from 'ajv-formats';
import CustomQueryBuilder from './custom_query_builder';
import knex from '../common/utils/db_connection';

export default class BaseModel extends Model {
  static QueryBuilder = CustomQueryBuilder;

  static softDelete = true; // by default, all models are soft deleted

  static delColumn = 'delete_at';

  static get idColumn(): string | string[] {
    return 'id';
  }

  static createValidator() {
    return new AjvValidator({
      onCreateAjv: (ajv) => {
        addFormats(ajv);
      },
      options: {
        $data: true,
        allErrors: true,
        validateSchema: false,
        ownProperties: true,
        // v5: true,
        coerceTypes: true,
        removeAdditional: true,
      },
    });
  }
  // static customMethod() {
  //   console.log('base customMethod');
  // }

  static isSoftDelete() {
    return this.softDelete;
  }


}

BaseModel.knex(knex);
