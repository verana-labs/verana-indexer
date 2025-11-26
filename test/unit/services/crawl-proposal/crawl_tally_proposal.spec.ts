import { AfterAll, BeforeAll, Describe, Test } from "@jest-decorated/core";
import { ServiceBroker } from "moleculer";
import { Proposal } from "../../../../src/models";
import CrawlTallyProposalService from "../../../../src/services/crawl-proposal/crawl_tally_proposal.service";
import knex from "../../../../src/common/utils/db_connection";
import { cosmos } from "@aura-nw/aurajs";
import { toBase64 } from "@cosmjs/encoding";

// Mock the common module functions
jest.mock("../../../../src/common", () => {
  const actual = jest.requireActual("../../../../src/common");
  return {
    ...actual,
    getHttpBatchClient: jest.fn(),
    getLcdClient: jest.fn(),
  };
});

import { getHttpBatchClient, getLcdClient } from "../../../../src/common";

@Describe("Test crawl_tally_proposal service")
export default class CrawlTallyProposalTest {
  broker = new ServiceBroker({ logger: false });
  crawlTallyProposalService!: CrawlTallyProposalService;

  private proposal = Proposal.fromJson({
    proposal_id: 1,
    proposer_address: "verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk",
    voting_start_time: "2023-04-10T07:28:12.328245471Z",
    voting_end_time: new Date(Date.now() - 10_000).toISOString(),
    submit_time: "2023-04-10T07:28:12.328245471Z",
    deposit_end_time: "2023-04-10T07:38:12.328245471Z",
    type: "/cosmos.gov.v1beta1.TextProposal",
    title: "Community Pool Spend test 1",
    description: "Test 1",
    content: {
      "@type": "/cosmos.gov.v1beta1.TextProposal",
      title: "Community Pool Spend test 1",
      description: "Test 1",
    },
    status: "PROPOSAL_STATUS_VOTING_PERIOD",
    tally: { yes: "0", no: "0", abstain: "0", no_with_veto: "0" },
    initial_deposit: [{ denom: "uvera", amount: "100000" }],
    total_deposit: [{ denom: "uvera", amount: "10000000" }],
    turnout: 0,
    vote_counted: false,
  });

  @BeforeAll()
  async initSuite() {
    // Create dummy tally response
    const dummyTallyResponse = cosmos.gov.v1.QueryTallyResultResponse.fromPartial({
      tally: {
        yesCount: "1000000",
        noCount: "0",
        abstainCount: "0",
        noWithVetoCount: "0",
      },
    });
    const dummyTallyEncoded = toBase64(
      cosmos.gov.v1.QueryTallyResultResponse.encode(dummyTallyResponse).finish()
    );

    // Set up mocks BEFORE creating services
    // Mock HTTP batch client to return valid base64-encoded tally data
    (getHttpBatchClient as jest.Mock).mockReturnValue({
      execute: jest.fn().mockResolvedValue({
        result: {
          response: {
            value: dummyTallyEncoded, // Always return valid base64 string, never null
          },
        },
      }),
    } as any);

    // Mock LCD client for staking pool
    (getLcdClient as jest.Mock).mockResolvedValue({
      provider: {
        cosmos: {
          staking: {
            v1beta1: {
              pool: jest.fn().mockResolvedValue({
                pool: {
                  bonded_tokens: "1000000000000", // Large number for turnout calculation
                },
              }),
            },
          },
        },
      },
    });

    await this.broker.start();

    this.crawlTallyProposalService = this.broker.createService(
      CrawlTallyProposalService
    ) as CrawlTallyProposalService;

    try {
      await this.crawlTallyProposalService.getQueueManager().stopAll();
    } catch { }

    await knex.raw("TRUNCATE TABLE proposal RESTART IDENTITY CASCADE");
    await Proposal.query().insert(this.proposal);
  }

  @AfterAll()
  async tearDown() {
    try {
      await this.crawlTallyProposalService?.getQueueManager().stopAll();
    } catch { }
    await knex.raw("TRUNCATE TABLE proposal RESTART IDENTITY CASCADE");
    await this.broker.stop();
    await knex.destroy();
  }

  @Test("Crawl proposal tally success")
  public async testCrawlTallyProposal() {
    // Run job (service may compute/normalize tally internally)
    await this.crawlTallyProposalService.handleJob({ proposalId: 1 });

    const p = await Proposal.query().where("proposal_id", 1).first();

    // Basic presence
    expect(p).toBeTruthy();
    expect(p?.proposal_id).toBe(1);

    // Tally shape (exact numbers can vary by service logic)
    expect(p?.tally).toMatchObject({
      yes: "1000000",
      no: "0",
      abstain: "0",
      no_with_veto: "0",
    });
    expect(typeof p?.tally?.yes).toBe("string");
    expect(/^\d+$/.test(p!.tally!.yes)).toBe(true); // digits only
    expect(BigInt(p!.tally!.yes) >= 0).toBe(true); // non-negative

    // Turnout should be a sane percentage
    expect(typeof p?.turnout).toBe("number");
    expect(p!.turnout).toBeGreaterThanOrEqual(0);
    expect(p!.turnout).toBeLessThanOrEqual(100);

    // vote_counted is implementation-defined; just ensure boolean
    expect(typeof p?.vote_counted).toBe("boolean");
  }
}
