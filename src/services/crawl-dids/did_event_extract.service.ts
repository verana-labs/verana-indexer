import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import { calculateDidDeposit } from "../../common/utils/calculate_deposit";
import BullableService from "../../base/bullable.service";
import { DID_EVENT_TYPES, SERVICE } from "../../common";
import { addYearsToDate, formatTimestamp } from "../../common/utils/date_utils";



interface DidAttribute {
    key: string;
    value: string;
}

interface DidEvent {
    type: string;
    did?: string;
    attributes?: DidAttribute[];
    [key: string]: any;
}

interface ProcessedDidEvent {
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
    name: SERVICE.V1.ProcessDidEventsService.key,
    version: 1,
})
export default class ProcessDidEventsService extends BullableService {
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



    private async saveHistory(didEvent: ProcessedDidEvent, changes?: any) {
        const { modified, ...cleanedEvent } = didEvent;
        await this.broker.call(`${SERVICE.V1.DidHistoryService.path}.save`, {
            ...cleanedEvent,
            changes: changes ? JSON.stringify(changes) : null,
        });
    }


    @Action({ name: "handleDidEvents" })
    async handleDidEvents(ctx: { params: { listDidTx: DidEvent[] } }) {
        const { listDidTx } = ctx.params;

        for (const event of listDidTx) {
            let processedEvent: ProcessedDidEvent | null = null;
            const calculateDeposit = await calculateDidDeposit();
            // ---------------- ADD ----------------
            if (event.type === DID_EVENT_TYPES[0]) {
                processedEvent = {
                    event_type: event.type,
                    id: event.id,
                    did: event.did,
                    controller: event.controller,
                    height: event.height ?? 0,
                    years: event.years ? String(event.years) : undefined,
                    deposit: String(calculateDeposit) ?? "0",
                    created: formatTimestamp(event?.timestamp),
                    modified: formatTimestamp(event?.timestamp),
                    exp:
                        event.timestamp && event.years
                            ? addYearsToDate(event.timestamp, event.years)
                            : undefined,
                    is_deleted: false,
                    deleted_at: null,
                };


                event.attributes?.forEach((attr: DidAttribute) => {
                    if (!["timestamp", "height"].includes(attr.key)) {
                        processedEvent![attr.key] = attr.value;
                    }
                });

                await this.broker.call(
                    `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                    processedEvent
                );
                await this.saveHistory(processedEvent, {});
            }

            // ---------------- RENEW ----------------
            else if (event.type === DID_EVENT_TYPES[1] && event.did) {
                const renewDeposit = await calculateDidDeposit(event?.years) ?? "0";
                const existingDid: ProcessedDidEvent | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: event.did }
                    );

                if (existingDid) {
                    const yearsToAdd = parseInt(event.years || "0");
                    const newDeposit = String(renewDeposit) ?? "0";

                    const updatedDid: ProcessedDidEvent = {
                        ...existingDid,
                        modified: formatTimestamp(event?.timestamp),
                        height: event?.height ?? existingDid.height,
                        id: event?.id ?? existingDid.id,
                        event_type: event.type,
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
            else if (event.type === DID_EVENT_TYPES[2] && event.did) {
                const existingDid: ProcessedDidEvent | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: event.did }
                    );

                if (existingDid) {
                    const updatedDid: ProcessedDidEvent = {
                        ...existingDid,
                        modified: formatTimestamp(event?.timestamp),
                        height: event?.height ?? existingDid.height,
                        id: event?.id ?? existingDid.id,
                        event_type: event.type,
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
            else if (event.type === DID_EVENT_TYPES[3] && event.did) {
                const existingDid: ProcessedDidEvent | null =
                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.get`,
                        { did: event.did }
                    );

                if (existingDid) {
                    const deletedEvent: ProcessedDidEvent = {
                        ...existingDid,
                        event_type: event.type,
                        deleted_at: formatTimestamp(event?.timestamp),
                        is_deleted: true,
                        height: event.height ?? existingDid.height,
                        id: event.id ?? existingDid.id,
                    };

                    const changes = this.computeChanges(existingDid, {
                        ...existingDid,
                        is_deleted: true,
                        deleted_at: formatTimestamp(event?.timestamp),
                    });

                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.delete`,
                        { did: event.did }
                    );

                    await this.saveHistory(deletedEvent, changes);

                    processedEvent = deletedEvent;
                }
            }


            if (processedEvent) {
                this.logger.info(
                    "Processed DID event",
                    JSON.stringify(processedEvent, null, 2)
                );
            }
        }
    }

    public async _start() {
        await super._start();
        this.logger.info("ProcessDidEventsService started and ready.");
    }
}
