jest.mock('../../../../src/common/utils/params_service', () => ({
  getModuleParamsAction: jest.fn(),
}))

import { ServiceBroker } from 'moleculer'
import { MODULE_DISPLAY_NAMES, ModulesParamsNamesTypes } from '../../../../src/common'
import { getModuleParamsAction } from '../../../../src/common/utils/params_service'
import CorporationApiService from '../../../../src/services/crawl-co/co_api.service'

describe('CorporationApiService.getCorporationParams', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new CorporationApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    ;(getModuleParamsAction as jest.Mock).mockResolvedValue({ params: {} })
  })

  it('delegates to the co module params helper and returns its result', async () => {
    const ctx: any = { params: {}, meta: {} }

    const res = await service.getCorporationParams(ctx)

    expect(getModuleParamsAction).toHaveBeenCalledWith(
      ctx,
      ModulesParamsNamesTypes.CO,
      MODULE_DISPLAY_NAMES.CORPORATION
    )
    expect(res).toEqual({ params: {} })
  })

  it('forwards the ctx (carrying At-Block-Height in meta) so the helper can resolve point-in-time', async () => {
    const ctx: any = { params: {}, meta: { blockHeight: 42 } }

    await service.getCorporationParams(ctx)

    expect(getModuleParamsAction).toHaveBeenCalledWith(
      ctx,
      ModulesParamsNamesTypes.CO,
      MODULE_DISPLAY_NAMES.CORPORATION
    )
  })
})
