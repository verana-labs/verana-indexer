import { Knex } from 'knex';
import _ from 'lodash';
import { EVMContractVerification } from '../../src/models/evm_contract_verification';
import bank from '../../src/services/evm/precompile-abis/bank.json' with { type: 'json' };
import bech32 from '../../src/services/evm/precompile-abis/bech32.json' with { type: 'json' };
import distribution from '../../src/services/evm/precompile-abis/distribution.json' with { type: 'json' };
import ics20 from '../../src/services/evm/precompile-abis/ics20.json' with { type: 'json' };
import staking from '../../src/services/evm/precompile-abis/staking.json' with { type: 'json' };
import vesting from '../../src/services/evm/precompile-abis/vesting.json' with { type: 'json' };

// https://github.com/evmos/evmos/tree/main/precompiles
const precompileContracts = [
  {
    address: '0x0000000000000000000000000000000000000400',
    type: 'PRECOMPILE_BECH32',
    abi: JSON.stringify(bech32),
  },
  {
    address: '0x0000000000000000000000000000000000000800',
    type: 'PRECOMPILE_STAKING',
    abi: JSON.stringify(staking),
  },
  {
    address: '0x0000000000000000000000000000000000000801',
    type: 'PRECOMPILE_DISTRIBUTION',
    abi: JSON.stringify(distribution),
  },
  {
    address: '0x0000000000000000000000000000000000000802',
    type: 'PRECOMPILE_ICS20',
    abi: JSON.stringify(ics20),
  },
  {
    address: '0x0000000000000000000000000000000000000803',
    type: 'PRECOMPILE_VESTING',
    abi: JSON.stringify(vesting),
  },
  {
    address: '0x0000000000000000000000000000000000000804',
    type: 'PRECOMPILE_BANK',
    abi: JSON.stringify(bank),
  },
];
const precompileAddresses = _.map(precompileContracts, 'address');

export async function up(knex: Knex): Promise<void> {
  await knex('evm_smart_contract')
    .insert(
      _.map(precompileContracts, (contract) => ({
        address: contract.address,
        type: contract.type,
      }))
    )
    .onConflict('address')
    .merge();

  await knex('evm_contract_verification').insert(
    _.map(precompileContracts, (contract) => ({
      contract_address: contract.address,
      abi: contract.abi,
      status: EVMContractVerification.VERIFICATION_STATUS.SUCCESS,
    }))
  );
}

export async function down(knex: Knex): Promise<void> {
  await knex('evm_smart_contract')
    .where('address', 'IN', precompileAddresses)
    .del();

  await knex('evm_contract_verification')
    .where('contract_address', 'IN', precompileAddresses)
    .del();
}
