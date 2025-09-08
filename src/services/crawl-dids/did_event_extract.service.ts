import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { addYearsToDate, formatTimestamp } from "../../common/utils/date_utils";

interface DidAttribute {
    key: string;
    value: string;
}

interface DidEvent {
    type: 'add_did' | 'touch_did' | 'renew_did' | 'remove_did' | string;
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

    @Action({ name: "handleDidEvents" })
    async handleDidEvents(ctx: { params: { listDidTx: DidEvent[]; blockHeight: number; timestamp: string | number } }) {
        const { listDidTx, blockHeight, timestamp } = ctx.params;

        const processedDidEvents: ProcessedDidEvent[] = listDidTx?.map((event: DidEvent) => {
            const flatObj: ProcessedDidEvent = {
                event_type: event.type,
            };

            if (event.type === 'add_did') {
                flatObj.created = formatTimestamp(timestamp);
                flatObj.modified = formatTimestamp(timestamp);
                flatObj.deleted_at = null;
                flatObj.height = blockHeight;
                flatObj.is_deleted = false;
            }

            if (event.type === 'touch_did') {
                flatObj.modified = formatTimestamp(timestamp);
                flatObj.height = blockHeight;
            }

            event.attributes?.forEach((attr: DidAttribute) => {
                if (attr.key === 'expiration') {
                    flatObj.exp = formatTimestamp(attr.value);
                } else if (!['msg_index', 'timestamp', 'block_height'].includes(attr.key)) {
                    flatObj[attr.key] = attr.value;
                }
            });

            if (event.did) flatObj.did = event.did;

            return flatObj;
        });

        for (const didEvent of processedDidEvents) {
            if (['add_did', "touch_did"].includes(didEvent.event_type)) {
                console.log(didEvent, "customlog");
                await this.broker.call(
                    `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                    didEvent
                );
            }
            else if (didEvent.event_type === 'renew_did' && didEvent.did) {
                const existingDid: ProcessedDidEvent | null = await this.broker.call(
                    `${SERVICE.V1.DidDatabaseService.path}.get`,
                    { did: didEvent.did }
                );

                if (existingDid) {
                    existingDid.modified = formatTimestamp(timestamp);
                    existingDid.height = blockHeight;
                    existingDid.event_type = didEvent.event_type;
                    const yearsToAdd = parseInt(didEvent.years || "0");
                    existingDid.exp = addYearsToDate(existingDid.exp, yearsToAdd);
                    const incomingDeposit = parseInt(didEvent.deposit || "0");
                    const existingDeposit = parseInt(existingDid.deposit || "0");
                    existingDid.deposit = (existingDeposit + incomingDeposit).toString();

                    await this.broker.call(
                        `${SERVICE.V1.DidDatabaseService.path}.upsertProcessedDid`,
                        existingDid
                    );
                }
            }
            else if (didEvent.event_type === 'remove_did' && didEvent.did) {
                await this.broker.call(`${SERVICE.V1.DidDatabaseService.path}.delete`, { did: didEvent.did });
            }
        }

        this.logger.info(
            'Processed and saved DID events to database.',
            JSON.stringify(processedDidEvents, null, 2)
        );
    }

    public async _start() {
        await super._start();
        this.logger.info("ProcessDidEventsService started and ready.");
    }
}
