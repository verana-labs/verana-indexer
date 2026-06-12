import { ServiceBroker } from "moleculer";
import ParticipantProcessorService from "../../../../src/services/crawl-pp/pp_processor.service";
import {
  SERVICE,
} from "../../../../src/common/constant";
import { VeranaParticipantMessageTypes as ParticipantMessageTypes } from "../../../../src/common/verana-message-types";
import {
  fetchParticipant,
  fetchParticipantSession,
} from "../../../../src/modules/pp-height-sync/pp_height_sync_helpers";

jest.mock("../../../../src/common/utils/start_mode_detector", () => ({
  detectStartMode: jest.fn().mockResolvedValue({ isFreshStart: false })
}));

jest.mock("../../../../src/modules/pp-height-sync/pp_height_sync_helpers", () => {
  const actual = jest.requireActual(
    "../../../../src/modules/pp-height-sync/pp_height_sync_helpers"
  );
  return {
    __esModule: true,
    ...actual,
    fetchParticipant: jest.fn(),
    fetchParticipantSession: jest.fn(),
  };
});

describe("🧪 ParticipantProcessorService", () => {
  let broker: ServiceBroker;
  let oldUseHeightSyncParticipant: string | undefined;
  let syncParticipantFromLedgerSpy: jest.Mock;
  let compareParticipantWithLedgerSpy: jest.Mock;

  // Keep references to spies
  let spyCreateRootParticipant: jest.Mock;
  let spyCreateParticipant: jest.Mock;

  beforeAll(async () => {
    oldUseHeightSyncParticipant = process.env.USE_HEIGHT_SYNC_PARTICIPANT;
    process.env.USE_HEIGHT_SYNC_PARTICIPANT = "false";
    broker = new ServiceBroker({ logger: false });

    // ✅ Create spies BEFORE creating the service
    spyCreateRootParticipant = jest.fn(() => ({ saved: true }));
    spyCreateParticipant = jest.fn(() => ({ saved: true }));
    syncParticipantFromLedgerSpy = jest.fn(() => ({ success: true, schemaId: 7 }));
    compareParticipantWithLedgerSpy = jest.fn(() => ({ success: true, matches: true }));

    broker.createService({
      name: "participantIngest",
      actions: {
        handleMsgCreateRootParticipant: spyCreateRootParticipant,
        handleMsgSelfCreateParticipant: spyCreateParticipant,
        handleMsgSetParticipantEffectiveUntil: jest.fn(() => ({ saved: true })),
        handleMsgRevokeParticipant: jest.fn(() => ({ saved: true })),
        handleMsgStartParticipantOP: jest.fn(() => ({ saved: true })),
        handleMsgSetParticipantOPToValidated: jest.fn(() => ({ saved: true })),
        handleMsgRenewParticipantOP: jest.fn(() => ({ saved: true })),
        handleMsgCancelParticipantOPLastRequest: jest.fn(() => ({
          saved: true,
        })),
        handleMsgCreateOrUpdateParticipantSession: jest.fn(() => ({
          saved: true,
        })),
        handleMsgSlashParticipantTrustDeposit: jest.fn(() => ({ saved: true })),
        handleMsgRepayParticipantSlashedTrustDeposit: jest.fn(() => ({
          saved: true,
        })),
        syncParticipantFromLedger: syncParticipantFromLedgerSpy,
        syncParticipantSessionFromLedger: jest.fn(() => ({ success: true })),
        compareParticipantWithLedger: compareParticipantWithLedgerSpy,
        compareParticipantSessionWithLedger: jest.fn(() => ({ success: true, matches: true })),
        getParticipant: jest.fn(() => ({ id: "participant-123", type: "root" })),
        listParticipants: jest.fn(() => [
          { id: "participant-1", type: "root" },
          { id: "participant-2", type: "issue" },
        ]),
      },
    });

    broker.createService({
      name: SERVICE.V1.TrustDepositDatabaseService.key,
      version: 1,
      actions: {
        syncFromLedger: jest.fn(() => ({ success: true })),
      },
    });

    broker.createService({
      name: SERVICE.V1.EcosystemDatabaseService.key,
      version: 1,
      actions: {
        get: jest.fn(() => ({ ecosystem: { id: 3, controller: "verana1controller" } })),
      },
    });

    broker.createService({
      name: SERVICE.V1.CredentialSchemaDatabaseService.key,
      version: 1,
      actions: {
        syncFromLedger: jest.fn(() => ({ success: true })),
      },
    });

    broker.createService(ParticipantProcessorService);
    await broker.start();
  }, 30000);

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_PARTICIPANT = oldUseHeightSyncParticipant;
    await broker.stop();
  }, 30000);

  it("✅ should process participant messages and call correct handlers", async () => {
    const messages = [
      {
        type: ParticipantMessageTypes.CreateRootParticipant,
        content: { "@type": "someType", id: "participant1", corporation: "acc1" },
        timestamp: "2025-10-08T10:00:00Z",
      },
      {
        type: ParticipantMessageTypes.SelfCreateParticipant,
        content: { "@type": "someType", id: "participant2", corporation: "acc2" },
        timestamp: "2025-10-08T11:00:00Z",
      },
    ];

    await broker.call(
      `v1.${SERVICE.V1.ParticipantProcessorService.key}.handleParticipantMessages`,
      { participantMessages: messages }
    );

    // ✅ Check first spy call
    const ctxRoot = spyCreateRootParticipant.mock.calls[0][0];
    expect(ctxRoot.params.data.id).toBe("participant1");
    expect(ctxRoot.params.data.corporation).toBe("acc1");
    expect(ctxRoot.params.data).toHaveProperty("timestamp");

    // ✅ Check second spy call
    const ctxParticipant = spyCreateParticipant.mock.calls[0][0];
    expect(ctxParticipant.params.data.id).toBe("participant2");
    expect(ctxParticipant.params.data.corporation).toBe("acc2");
    expect(ctxParticipant.params.data).toHaveProperty("timestamp");
  });

  it("✅ should return participant for getParticipant", async () => {
    const res = await broker.call(
      `v1.${SERVICE.V1.ParticipantProcessorService.key}.getParticipant`,
      { schema_id: 123, grantee: "wallet123", type: "root" }
    );
    expect(res).toEqual({ id: "participant-123", type: "root" });
  });

  it("✅ should list participants for listParticipants", async () => {
    const res = await broker.call(
      `v1.${SERVICE.V1.ParticipantProcessorService.key}.listParticipants`,
      { schema_id: 123, grantee: "wallet123", type: "root" }
    );
    expect(Array.isArray(res)).toBe(true);
    expect(res.length).toBe(2);
    expect(res[0].id).toBe("participant-1");
  });

  it("✅ should use height-sync strategy when USE_HEIGHT_SYNC_PARTICIPANT=true", async () => {
    process.env.USE_HEIGHT_SYNC_PARTICIPANT = "true";
    (fetchParticipant as jest.Mock).mockResolvedValue({
      participant: {
        id: 101,
        schema_id: 7,
        role: "ISSUER",
        grantee: "verana1grantee",
        created_by: "verana1creator",
        created: "2026-03-01T00:00:00Z",
        modified: "2026-03-01T00:00:00Z",
        validation_fees: 0,
        issuance_fees: 0,
        verification_fees: 0,
        deposit: 0,
        slashed_deposit: 0,
        repaid_deposit: 0,
        op_state: "VALIDATED",
        op_validator_deposit: 0,
        op_current_fees: 0,
        op_current_deposit: 0,
      },
    });
    (fetchParticipantSession as jest.Mock).mockResolvedValue(null);

    await broker.call(
      `v1.${SERVICE.V1.ParticipantProcessorService.key}.handleParticipantMessages`,
      {
        participantMessages: [{
          type: ParticipantMessageTypes.SelfCreateParticipant,
          content: { id: 101 },
          height: 123,
          timestamp: "2026-03-01T00:00:00Z",
          txHash: "0xabc",
          txEvents: [],
        }],
      }
    );

    expect(syncParticipantFromLedgerSpy).toHaveBeenCalled();
    expect(compareParticipantWithLedgerSpy).toHaveBeenCalled();

    (fetchParticipant as jest.Mock).mockReset();
    (fetchParticipantSession as jest.Mock).mockReset();
    process.env.USE_HEIGHT_SYNC_PARTICIPANT = "false";
  });
});
