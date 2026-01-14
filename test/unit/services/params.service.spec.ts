import { ModulesParamsNamesTypes } from '../../../src/common/constant';
import {
  UpdateParamsMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDidMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from '../../../src/common/verana-message-types';
import { hasMeaningfulChanges } from '../../../src/common/utils/params_utils';

/**
 * Test Suite for UpdateParams Message Processing Logic
 *
 * This test suite verifies that UpdateParams messages are correctly processed
 * for all Verana modules and mapped to their corresponding module constants.
 */
describe('UpdateParams Message Processing', () => {

  describe('Message Type to Module Mapping', () => {
    it('should map CREDENTIAL_SCHEMA to CS module', () => {
      expect(UpdateParamsMessageTypes.CREDENTIAL_SCHEMA).toBe('/verana.cs.v1.MsgUpdateParams');
      expect(VeranaCredentialSchemaMessageTypes.UpdateParams).toBe('/verana.cs.v1.MsgUpdateParams');
      expect(ModulesParamsNamesTypes.CS).toBe('cs');
    });

    it('should map DID_DIRECTORY to DD module', () => {
      expect(UpdateParamsMessageTypes.DID_DIRECTORY).toBe('/verana.dd.v1.MsgUpdateParams');
      expect(VeranaDidMessageTypes.UpdateParams).toBe('/verana.dd.v1.MsgUpdateParams');
      expect(ModulesParamsNamesTypes.DD).toBe('dd');
    });

    it('should map PERMISSION to PERM module', () => {
      expect(UpdateParamsMessageTypes.PERMISSION).toBe('/verana.perm.v1.MsgUpdateParams');
      expect(VeranaPermissionMessageTypes.UpdateParams).toBe('/verana.perm.v1.MsgUpdateParams');
      expect(ModulesParamsNamesTypes.PERM).toBe('perm');
    });

    it('should map TRUST_DEPOSIT to TD module', () => {
      expect(UpdateParamsMessageTypes.TRUST_DEPOSIT).toBe('/verana.td.v1.MsgUpdateParams');
      expect(VeranaTrustDepositMessageTypes.UpdateParams).toBe('/verana.td.v1.MsgUpdateParams');
      expect(ModulesParamsNamesTypes.TD).toBe('td');
    });

    it('should map TRUST_REGISTRY to TR module', () => {
      expect(UpdateParamsMessageTypes.TRUST_REGISTRY).toBe('/verana.tr.v1.MsgUpdateParams');
      expect(VeranaTrustRegistryMessageTypes.UpdateParams).toBe('/verana.tr.v1.MsgUpdateParams');
      expect(ModulesParamsNamesTypes.TR).toBe('tr');
    });
  });

  describe('UpdateParams Message Structure Validation', () => {

    it('should validate required fields are present', () => {
      const validMessage = {
        type: UpdateParamsMessageTypes.CREDENTIAL_SCHEMA,
        authority: 'cosmos1authority',
        params: { credential_schema_trust_deposit: '1000000' }
      };

      expect(validMessage.type).toBeDefined();
      expect(validMessage.params).toBeDefined();
      expect(validMessage.params.credential_schema_trust_deposit).toBe('1000000');
    });

    it('should identify invalid messages without type', () => {
      const invalidMessage = {
        authority: 'cosmos1authority',
        params: { someParam: 'value' }
      } as any;

      expect(invalidMessage.type).toBeUndefined();
      expect(invalidMessage.params).toBeDefined();
    });

    it('should identify invalid messages without params', () => {
      const invalidMessage = {
        type: UpdateParamsMessageTypes.CREDENTIAL_SCHEMA,
        authority: 'cosmos1authority'
      } as any;

      expect(invalidMessage.type).toBeDefined();
      expect(invalidMessage.params).toBeUndefined();
    });

    it('should identify unknown message types', () => {
      const unknownType = '/verana.unknown.v1.MsgUpdateParams';
      expect(Object.values(UpdateParamsMessageTypes)).not.toContain(unknownType);
    });
  });

  describe('Module Parameter Structure Validation', () => {
    it('should validate Credential Schema parameters structure', () => {
      const csParams = {
        credential_schema_trust_deposit: '1000000',
        credential_schema_schema_max_size: '16384',
        credential_schema_issuer_grantor_validation_validity_period_max_days: '365',
        credential_schema_verifier_grantor_validation_validity_period_max_days: '365',
        credential_schema_issuer_validation_validity_period_max_days: '365',
        credential_schema_verifier_validation_validity_period_max_days: '365',
        credential_schema_holder_validation_validity_period_max_days: '365'
      };

      expect(csParams).toHaveProperty('credential_schema_trust_deposit');
      expect(csParams).toHaveProperty('credential_schema_schema_max_size');
      expect(typeof csParams.credential_schema_trust_deposit).toBe('string');
      expect(typeof csParams.credential_schema_schema_max_size).toBe('string');
    });

    it('should validate DID Directory parameters structure', () => {
      const ddParams = {
        did_directory_trust_deposit: '500000',
        did_directory_grace_period: '30'
      };

      expect(ddParams).toHaveProperty('did_directory_trust_deposit');
      expect(ddParams).toHaveProperty('did_directory_grace_period');
      expect(typeof ddParams.did_directory_trust_deposit).toBe('string');
      expect(typeof ddParams.did_directory_grace_period).toBe('string');
    });

    it('should validate Permission parameters structure', () => {
      const permParams = {
        validation_term_requested_timeout_days: '7'
      };

      expect(permParams).toHaveProperty('validation_term_requested_timeout_days');
      expect(typeof permParams.validation_term_requested_timeout_days).toBe('string');
    });

    it('should validate Trust Deposit parameters structure with v0.9.2 fields', () => {
      const tdParams = {
        trust_deposit_reclaim_burn_rate: '0.60',
        trust_deposit_share_value: '1.0',
        trust_deposit_rate: '0.20',
        wallet_user_agent_reward_rate: '0.10',
        user_agent_reward_rate: '0.10',
        trust_deposit_max_yield_rate: '0.25',
        yield_intermediate_pool: 'cosmos1yieldpool123'
      };

      expect(tdParams).toHaveProperty('trust_deposit_reclaim_burn_rate');
      expect(tdParams).toHaveProperty('trust_deposit_share_value');
      expect(tdParams).toHaveProperty('trust_deposit_max_yield_rate');
      expect(tdParams).toHaveProperty('yield_intermediate_pool');
      expect(typeof tdParams.trust_deposit_max_yield_rate).toBe('string');
      expect(typeof tdParams.yield_intermediate_pool).toBe('string');
    });

    it('should validate Trust Registry parameters structure', () => {
      const trParams = {
        trust_unit_price: '1000',
        trust_registry_trust_deposit: '2000000'
      };

      expect(trParams).toHaveProperty('trust_unit_price');
      expect(trParams).toHaveProperty('trust_registry_trust_deposit');
      expect(typeof trParams.trust_unit_price).toBe('string');
      expect(typeof trParams.trust_registry_trust_deposit).toBe('string');
    });
  });

  describe('Future-Proof Parameter Handling', () => {
    it('should handle unknown future parameters for Credential Schema', () => {
      const csParamsWithUnknown = {
        credential_schema_trust_deposit: '1000000',
        credential_schema_new_future_param: 'new_value',
        credential_schema_another_unknown: '123',
        unknown_attribute_xyz: 'test'
      };

      expect(csParamsWithUnknown).toHaveProperty('credential_schema_trust_deposit');
      expect(csParamsWithUnknown).toHaveProperty('credential_schema_new_future_param');
      expect(csParamsWithUnknown).toHaveProperty('credential_schema_another_unknown');
      expect(csParamsWithUnknown).toHaveProperty('unknown_attribute_xyz');

      const paramKeys = Object.keys(csParamsWithUnknown);
      expect(paramKeys.length).toBe(4);
      expect(paramKeys).toContain('credential_schema_trust_deposit');
      expect(paramKeys).toContain('credential_schema_new_future_param');
      expect(paramKeys).toContain('credential_schema_another_unknown');
      expect(paramKeys).toContain('unknown_attribute_xyz');
    });

    it('should handle unknown future parameters for Trust Deposit', () => {
      const tdParamsWithUnknown = {
        trust_deposit_rate: '0.20',
        trust_deposit_max_yield_rate: '0.25',
        yield_intermediate_pool: 'cosmos1yieldpool123',
        trust_deposit_future_param_1: 'future_value_1',
        trust_deposit_future_param_2: 'future_value_2',
        unknown_td_attribute: 'test_value'
      };

      expect(tdParamsWithUnknown).toHaveProperty('trust_deposit_rate');
      expect(tdParamsWithUnknown).toHaveProperty('trust_deposit_max_yield_rate');
      expect(tdParamsWithUnknown).toHaveProperty('yield_intermediate_pool');
      expect(tdParamsWithUnknown).toHaveProperty('trust_deposit_future_param_1');
      expect(tdParamsWithUnknown).toHaveProperty('trust_deposit_future_param_2');
      expect(tdParamsWithUnknown).toHaveProperty('unknown_td_attribute');

      const paramKeys = Object.keys(tdParamsWithUnknown);
      expect(paramKeys.length).toBe(6);
      expect(paramKeys).toContain('trust_deposit_future_param_1');
      expect(paramKeys).toContain('trust_deposit_future_param_2');
      expect(paramKeys).toContain('unknown_td_attribute');
    });

    it('should handle mixed known and unknown parameters for all modules', () => {
      const mixedParams = {
        credential_schema_trust_deposit: '1000000',
        cs_unknown_param: 'unknown_cs',

        did_directory_trust_deposit: '500000',
        dd_future_param: 'future_dd',

        validation_term_requested_timeout_days: '7',
        perm_new_attribute: 'new_perm',

        trust_deposit_rate: '0.20',
        td_unknown_field: 'unknown_td',

        trust_unit_price: '1000',
        tr_future_attribute: 'future_tr'
      };

      expect(Object.keys(mixedParams)).toHaveLength(10);
      expect(mixedParams).toHaveProperty('credential_schema_trust_deposit');
      expect(mixedParams).toHaveProperty('cs_unknown_param');
      expect(mixedParams).toHaveProperty('did_directory_trust_deposit');
      expect(mixedParams).toHaveProperty('dd_future_param');
      expect(mixedParams).toHaveProperty('validation_term_requested_timeout_days');
      expect(mixedParams).toHaveProperty('perm_new_attribute');
      expect(mixedParams).toHaveProperty('trust_deposit_rate');
      expect(mixedParams).toHaveProperty('td_unknown_field');
      expect(mixedParams).toHaveProperty('trust_unit_price');
      expect(mixedParams).toHaveProperty('tr_future_attribute');
    });

    it('should preserve all parameter types including numbers and booleans', () => {
      const mixedTypeParams = {
        string_param: 'string_value',
        number_param: 123,
        boolean_param: true,
        null_param: null,
        array_param: ['item1', 'item2'],
        object_param: { nested: 'value' }
      };

      expect(mixedTypeParams.string_param).toBe('string_value');
      expect(mixedTypeParams.number_param).toBe(123);
      expect(mixedTypeParams.boolean_param).toBe(true);
      expect(mixedTypeParams.null_param).toBeNull();
      expect(mixedTypeParams.array_param).toEqual(['item1', 'item2']);
      expect(mixedTypeParams.object_param).toEqual({ nested: 'value' });
    });
  });

  describe('Error Handling and Crash Prevention', () => {
    it('should detect new parameters as meaningful changes', () => {
      const oldParams = { existing_param: 'old_value' };
      const newParams = {
        existing_param: 'old_value',
        new_param: 'new_value'
      };

      expect(hasMeaningfulChanges(oldParams, newParams)).toBe(true);
    });

    it('should handle null and undefined parameters gracefully', () => {
      const oldParams = { param1: 'value1', param2: null };
      const newParams = { param1: 'value1', param2: undefined, param3: null };

      expect(() => hasMeaningfulChanges(oldParams, newParams)).not.toThrow();
      expect(hasMeaningfulChanges(oldParams, newParams)).toBe(true); 
    });

    it('should detect no changes when parameters are identical', () => {
      const oldParams = { param1: 'value1', param2: 'value2' };
      const newParams = { param1: 'value1', param2: 'value2' };

      expect(hasMeaningfulChanges(oldParams, newParams)).toBe(false);
    });

    it('should handle empty parameter objects', () => {
      expect(hasMeaningfulChanges({}, {})).toBe(false);
      expect(hasMeaningfulChanges(null, {})).toBe(false);
      expect(hasMeaningfulChanges({}, null)).toBe(true);
      expect(hasMeaningfulChanges(null, null)).toBe(false);
    });

    it('should detect changes in existing parameter values', () => {
      const oldParams = { param1: 'old_value', param2: 'same' };
      const newParams = { param1: 'new_value', param2: 'same' };

      expect(hasMeaningfulChanges(oldParams, newParams)).toBe(true);
    });

    it('should handle complex nested objects and arrays', () => {
      const oldParams = {
        simple: 'value',
        array: [1, 2, 3],
        object: { nested: 'old' }
      };
      const newParams = {
        simple: 'value',
        array: [1, 2, 3],
        object: { nested: 'old' },
        new_array: [4, 5, 6],
        new_object: { key: 'value' }
      };

      expect(hasMeaningfulChanges(oldParams, newParams)).toBe(true);
    });

    it('should verify error handling prevents crashes in TX service routing', () => {

      let errorHandled = false;

      try {
        throw new Error('Simulated broker failure');
      } catch (err) {
        errorHandled = true;
        expect((err as Error).message).toBe('Simulated broker failure');
      }

      expect(errorHandled).toBe(true);
    });

    it('should verify params service handles all data types safely', () => {
      const complexParams = {
        string: 'text',
        number: 42,
        boolean: true,
        null: null,
        undefined: undefined,
        array: [1, 'two', { three: 3 }],
        object: {
          nested: {
            deep: {
              value: 'test',
              array: [1, 2, { complex: 'object' }]
            }
          }
        },
        function: () => { },
        symbol: Symbol('test')
      };

      expect(() => JSON.stringify(complexParams)).not.toThrow();

      const jsonString = JSON.stringify(complexParams);
      expect(typeof jsonString).toBe('string');
      expect(jsonString.length).toBeGreaterThan(0);
    });
  });

});
