import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { DidMessages, SERVICE } from "../../common";
import { calculateDidDeposit } from "../../common/utils/calculate_deposit";
import { addYearsToDate, formatTimestamp } from "../../common/utils/date_utils";



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



    private async saveHistory(did: DidMessageTypes, changes?: any) {
        const { modified, ...cleanedDID } = did;
        await this.broker.call(`${SERVICE.V1.DidHistoryService.path}.save`, {
            ...cleanedDID,
            changes: changes ? JSON.stringify(changes) : null,
        });
    }


    @Action({ name: "handleDidMessages" })
    async handleDidMessages(ctx: { params: { messages: DidMessageType[] } }) {
        const { messages } = ctx.params;

        for (const message of messages) {
            let processedDID: DidMessageTypes | null = null;
            const calculateDeposit = await calculateDidDeposit();
            // ---------------- ADD ----------------
            if ([DidMessages.AddDid, DidMessages.AddDidLegacy].includes(message.type as DidMessages)) {
                processedDID = {
                    event_type: message.type,
                    did: message.did,
                    controller: message.controller,
                    height: message.height ?? 0,
                    years: message.years ? String(message.years) : undefined,
                    deposit: String(calculateDeposit) ?? "0",
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
                await this.saveHistory(processedDID, {});
            }

            // ---------------- RENEW ----------------
            else if (
                (message.type === DidMessages.RenewDid || message.type === DidMessages.RenewDidLegacy)
                && message.did) {
                const renewDeposit = await calculateDidDeposit(message?.years) ?? "0";
                const existingDid: DidMessageTypes | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: message.did }
                    );

                if (existingDid) {
                    const yearsToAdd = parseInt(message.years || "0");
                    const newDeposit = String(renewDeposit) ?? "0";

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
                    await this.saveHistory(updatedDid, changes);
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
                    await this.saveHistory(updatedDid, changes);
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

                    await this.saveHistory(deletedDID, changes);

                    processedDID = deletedDID;
                }
            }


            if (processedDID) {
                this.logger.info(
                    "Processed DID Messages",
                    JSON.stringify(processedDID, null, 2)
                );
            }
        }
    }

    public async _start() {
        await super._start();
        this.logger.info("ProcessDidMessageService started and ready.");
    }
}
