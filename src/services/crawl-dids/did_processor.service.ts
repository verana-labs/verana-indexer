import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { VeranaDidMessageTypes } from "../../common/verana-message-types";
import { SERVICE } from "../../common";
import { calculateDidDeposit } from "../../common/utils/calculate_deposit";
import { addYearsToDate, formatTimestamp } from "../../common/utils/date_utils";
import { extractController } from "../../common/utils/extract_controller";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";



interface DidMessageType {
    type: string;
    did?: string;
    [key: string]: any;
}

interface DidMessageTypes {
    event_type: string;
    did?: string;
    created?: string;
    modified?: string;
    deleted_at?: string | null;
    height?: number;
    is_deleted?: boolean;
    exp?: string;
    deposit?: string;
    years?: string;
    id?: string;
    changes?: any;
    [key: string]: any;
}

@Service({
    name: SERVICE.V1.DidMessageProcessorService.key,
    version: 1,
})
export default class DidMessageProcessorService extends BullableService {
    private processorBase: MessageProcessorBase;
    private _isFreshStart: boolean = false;

    constructor(broker: ServiceBroker) {
        super(broker);
        this.processorBase = new MessageProcessorBase(this);
    }

    public async _start() {
        const startMode = await detectStartMode();
        this._isFreshStart = startMode.isFreshStart;
        this.processorBase.setFreshStartMode(this._isFreshStart);
        this.logger.info(`DID processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);
        await super._start();
        this.logger.info("ProcessDidMessageService started and ready.");
    }



    private async saveHistory(did: DidMessageTypes, height: number, changes?: any, isUpdate: boolean = false) {
        try {
            if (isUpdate && (!changes || Object.keys(changes).length === 0)) {
                return;
            }
            
            const { modified, id, ...cleanedDID } = did;
            const historyRecord = {
                ...cleanedDID,
                height: height, 
                changes: changes ? JSON.stringify(changes) : null,
            };
            await this.broker.call(`${SERVICE.V1.DidHistoryService.path}.save`, historyRecord);
        } catch (historyErr) {
            this.logger.error(`Failed to save DID history for ${did.did} at height ${height}:`, historyErr);
        }
    }


    @Action({ name: "handleDidMessages" })
    async handleDidMessages(ctx: { params: { messages: DidMessageType[] } }) {
        const { messages } = ctx.params;
        this.logger.info(`Processing ${messages.length} DID messages`);

        const processMessage = async (message: DidMessageType) => {
            this.logger.info(`Processing DID message: type=${message.type}, did=${message.did}, height=${message.height}`);
            let processedDID: DidMessageTypes | null = null;
            
            if ([VeranaDidMessageTypes.AddDid, VeranaDidMessageTypes.AddDidLegacy].includes(message.type as any)) {
                this.logger.info(`Creating new DID: ${message.did} at height ${message.height}`);
                const controller = extractController(message);
                if (!controller) {
                    this.logger.warn(`Missing controller/creator for DID ${message.did}, message keys: ${Object.keys(message).join(', ')}`);
                }
                
                const years = message.years ? (typeof message.years === 'string' ? parseInt(message.years, 10) : Number(message.years)) : 1;
                let depositAmount = 0;
                try {
                    depositAmount = await calculateDidDeposit(years);
                } catch (depositErr) {
                    this.logger.error(`Failed to calculate DID deposit:`, depositErr);
                }
                
                processedDID = {
                    event_type: message.type,
                    did: message.did,
                    controller: controller || null,
                    height: message.height ?? 0,
                    years: String(years),
                    deposit: String(depositAmount) ?? "0",
                    created: formatTimestamp(message?.timestamp),
                    modified: formatTimestamp(message?.timestamp),
                    exp:
                        message.timestamp && message.years
                            ? addYearsToDate(message.timestamp, message.years)
                            : undefined,
                    is_deleted: false,
                    deleted_at: null,
                };
                await this.broker.call(
                    `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                    processedDID
                );
                const blockHeight = message.height ?? 0;
                await this.saveHistory(processedDID, blockHeight, {});
            } else if (
                (message.type === VeranaDidMessageTypes.RenewDid || message.type === VeranaDidMessageTypes.RenewDidLegacy)
                && message.did) {
                const yearsToAdd = message.years 
                    ? (typeof message.years === 'string' ? parseInt(message.years, 10) : Number(message.years))
                    : 1;
                let renewDeposit = 0;
                try {
                    renewDeposit = await calculateDidDeposit(yearsToAdd) ?? 0;
                } catch (depositErr) {
                    this.logger.error(`Failed to calculate renew deposit:`, depositErr);
                }
                const existingDid: DidMessageTypes | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: message.did }
                    );

                if (existingDid) {
                    const newDeposit = String(renewDeposit);

                    const updatedDid: DidMessageTypes = {
                        ...existingDid,
                        modified: formatTimestamp(message?.timestamp),
                        height: message?.height ?? existingDid.height,
                        event_type: message.type,
                        exp: addYearsToDate(existingDid.exp, yearsToAdd),
                        deposit: (
                            parseInt(existingDid.deposit || "0") +
                            parseInt(newDeposit)
                        ).toString(),
                        years: (
                            parseInt(existingDid.years || "0") + yearsToAdd
                        ).toString(),
                    };

                    const changes = this.processorBase.computeChanges(
                        existingDid,
                        updatedDid
                    );

                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                        updatedDid
                    );
                    const blockHeight = message.height ?? 0;
                    await this.saveHistory(updatedDid, blockHeight, changes, true);
                }
            } else if (
                (message.type === VeranaDidMessageTypes.TouchDid || message.type === VeranaDidMessageTypes.TouchDidLegacy)
                && message.did
            ) {
                const existingDid: DidMessageTypes | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: message.did }
                    );
                if (existingDid) {
                    const updatedDid: DidMessageTypes = {
                        ...existingDid,
                        modified: formatTimestamp(message?.timestamp),
                        height: message?.height ?? existingDid.height,
                        event_type: message.type,
                    };

                    const changes = this.processorBase.computeChanges(
                        existingDid,
                        updatedDid
                    );
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                        updatedDid
                    );
                    const blockHeight = message.height ?? 0;
                    await this.saveHistory(updatedDid, blockHeight, changes, true);
                }
            } else if (
                (message.type === VeranaDidMessageTypes.RemoveDid || message.type === VeranaDidMessageTypes.RemoveDidLegacy)
                && message.did
            ) {
                const existingDid: DidMessageTypes | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: message.did }
                    );

                if (existingDid) {
                    const deletedDID: DidMessageTypes = {
                        ...existingDid,
                        event_type: message.type,
                        deleted_at: formatTimestamp(message?.timestamp),
                        is_deleted: true,
                        height: message.height ?? existingDid.height,
                    };

                    const changes = this.processorBase.computeChanges(existingDid, {
                        ...existingDid,
                        is_deleted: true,
                        deleted_at: formatTimestamp(message?.timestamp),
                    });

                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.delete`,
                        { did: message.did }
                    );

                    const blockHeight = message.height ?? 0;
                    await this.saveHistory(deletedDID, blockHeight, changes);

                    processedDID = deletedDID;
                }
            }

            if (processedDID) {
                this.logger.info(`Processed DID message for ${processedDID.did}`);
            }
        };

        await this.processorBase.processInBatches(
            messages,
            processMessage,
            {
                maxConcurrent: this._isFreshStart ? 3 : 8,
                batchSize: this._isFreshStart ? 20 : 50,
                delayBetweenBatches: this._isFreshStart ? 500 : 200,
            }
        );
    }
}
