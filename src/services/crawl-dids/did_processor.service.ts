import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { DidMessages, SERVICE } from "../../common";
import { calculateDidDeposit } from "../../common/utils/calculate_deposit";
import { addYearsToDate, formatTimestamp } from "../../common/utils/date_utils";
import { extractController, requireController } from "../../common/utils/extract_controller";



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
    constructor(broker: ServiceBroker) {
        super(broker);
    }


    private computeChanges(
        oldRec: Record<string, any>,
        newRec: Record<string, any>
    ): Record<string, { old: any; new: any }> {
        const diff: Record<string, { old: any; new: any }> = {};
        Object.keys(newRec).forEach((key) => {
            if (newRec[key] !== oldRec[key]) {
                diff[key] = { old: oldRec[key], new: newRec[key] };
            }
        });
        return diff;
    }



    private async saveHistory(did: DidMessageTypes, height: number, changes?: any, isUpdate: boolean = false) {
        try {
            if (isUpdate && (!changes || Object.keys(changes).length === 0)) {
                this.logger.info(`Skipping DID history - no actual changes for update at height: ${height}`);
                return;
            }
            
            const { modified, id, ...cleanedDID } = did;
            const historyRecord = {
                ...cleanedDID,
                height: height, 
                changes: changes ? JSON.stringify(changes) : null,
            };
            this.logger.info(`Saving DID history with height: ${height}`, historyRecord);
            await this.broker.call(`${SERVICE.V1.DidHistoryService.path}.save`, historyRecord);
        } catch (historyErr) {
            this.logger.error(`❌ Failed to save DID history for ${did.did} at height ${height}:`, historyErr);
            console.error("FATAL DID HISTORY SAVE ERROR:", historyErr);
        }
    }


    @Action({ name: "handleDidMessages" })
    async handleDidMessages(ctx: { params: { messages: DidMessageType[] } }) {
        const { messages } = ctx.params;
        this.logger.info(`🔄 Processing ${messages.length} DID messages`);

        for (const message of messages) {
            try {
                this.logger.info(`📝 Processing DID message: type=${message.type}, did=${message.did}, height=${message.height}`);
                let processedDID: DidMessageTypes | null = null;
                
                let depositAmount = 0;
                try {
                    depositAmount = await calculateDidDeposit();
                } catch (depositErr) {
                    this.logger.error(`❌ Failed to calculate DID deposit:`, depositErr);
                    console.error("FATAL DID DEPOSIT ERROR:", depositErr);
                    
                }
                
                // ---------------- ADD ----------------
                if ([DidMessages.AddDid, DidMessages.AddDidLegacy].includes(message.type as DidMessages)) {
                    this.logger.info(`🆕 Creating new DID: ${message.did} at height ${message.height}`);
                    const controller = extractController(message);
                    if (!controller) {
                        this.logger.warn(`⚠️ Missing controller/creator for DID ${message.did}, message keys: ${Object.keys(message).join(', ')}`);
                    }
                    processedDID = {
                    event_type: message.type,
                    did: message.did,
                    controller: controller || null,
                    height: message.height ?? 0,
                    years: message.years ? String(message.years) : undefined,
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
            }

            // ---------------- RENEW ----------------
            else if (
                (message.type === DidMessages.RenewDid || message.type === DidMessages.RenewDidLegacy)
                && message.did) {
                let renewDeposit = 0;
                try {
                    renewDeposit = await calculateDidDeposit(message?.years) ?? 0;
                } catch (depositErr) {
                    this.logger.error(`❌ Failed to calculate renew deposit:`, depositErr);
                    console.error("FATAL DID RENEW DEPOSIT ERROR:", depositErr);
                    
                }
                const existingDid: DidMessageTypes | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: message.did }
                    );

                if (existingDid) {
                    const yearsToAdd = parseInt(message.years || "0");
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

                    const changes = this.computeChanges(
                        existingDid,
                        updatedDid
                    );

                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                        updatedDid
                    );
                    const blockHeight = message.height ?? 0;
                    await this.saveHistory(updatedDid, blockHeight, changes, true); // isUpdate = true
                }
            }

            // ---------------- TOUCH ----------------
            else if (
                (message.type === DidMessages.TouchDid || message.type === DidMessages.TouchDidLegacy)
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

                    const changes = this.computeChanges(
                        existingDid,
                        updatedDid
                    );
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                        updatedDid
                    );
                    const blockHeight = message.height ?? 0;
                    await this.saveHistory(updatedDid, blockHeight, changes, true); // isUpdate = true
                }
            }

            // ---------------- REMOVE ----------------
            else if (
                (message.type === DidMessages.RemoveDid || message.type === DidMessages.RemoveDidLegacy)
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

                    const changes = this.computeChanges(existingDid, {
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
                this.logger.info(
                    "✅ Processed DID Messages",
                    JSON.stringify(processedDID, null, 2)
                );
            }
            } catch (msgErr) {
                this.logger.error(`❌ Error processing DID message for ${message.did}:`, msgErr);
                console.error("FATAL DID ERROR:", msgErr);
                
            }
        }
    }

    public async _start() {
        await super._start();
        this.logger.info("ProcessDidMessageService started and ready.");
    }
}
