import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import axios from 'axios';
import { ServiceBroker } from 'moleculer';
import config from '../../../config.json' with { type: 'json' };
import BullableService from '../../base/bullable.service';
import { SERVICE } from '../../common';
import { ReusableWebSocketClient } from '../../common/utils/websocket-client';


interface DIDDetails {
    did: string;
    controller?: string;
    exp?: string;
    deposit?: string;
    created?: Date;
    modified?: Date;
    msg_index?: string;
}
interface DIDEvent {
    type: string;
    attributes: { key: string; value: string }[];
    txHash: string;
    height: string;
}


@Service({
    name: SERVICE.V1.DIDEventListenerService.key,
    version: 1,
    dependencies: ['v1.DIDDatabaseService']
})
export default class DIDEventListenerService extends BullableService {
    private wsClient: ReusableWebSocketClient | null = null;
    public constructor(public broker: ServiceBroker) {
        super(broker);
    }

    public async _start(): Promise<void> {
        await super._start();
        this.logger.info('🚀 Starting DID Event Listener Service');
        await this.waitForServices('v1.DIDDatabaseService');
        this.wsClient = new ReusableWebSocketClient({
            subscriptionQuery: "tm.event='Tx'",
            onMessage: this.handleMessage.bind(this),
            logger: this.logger
        });

        this.wsClient.connect();
    }


    private async handleMessage(data: string): Promise<void> {
        try {
            const message = JSON.parse(data);
            if (message.result?.data?.type === 'tendermint/event/Tx') {
                const height = parseInt(message?.result?.data?.value?.TxResult?.height);
                const events = this.extractDIDEvents(message);
                this.logger.info(`🔍 Extracted ${JSON.stringify(events)} DID events from block at height ${height}`);
                if (events.length > 0) {
                    this.logger.info(`🎯 Found ${events.length} DID events at height ${height}`);
                    await this.processDIDEvents(events, height);
                }
            }
        } catch (error) {
            this.logger.error('💥 Error processing message:', error);
        }
    }

    private extractDIDEvents(message: any): DIDEvent[] {
        if (!message?.result?.data?.value?.TxResult?.result?.events) {
            this.logger.warn('⚠️ No events found in message');
            return [];
        }
        const txEvents = message.result.data.value.TxResult.result.events;
        const txHash = message.result.data.value.TxResult.tx;
        const height = message.result.data.value.TxResult.height;
        this.logger.info(`🔍 Extracting DID events from message: ${JSON.stringify(message.result.data.value.TxResult.result.events)}`);

        const didEvents: DIDEvent[] = [];

        for (const event of txEvents) {
            if (['add_did', 'remove_did', 'renew_did', 'touch_did']?.includes(event.type)) {
                const attributes = event.attributes.map((attr: any) => {
                    try {
                        return {
                            key: this.tryDecodeBase64(attr.key),
                            value: this.tryDecodeBase64(attr.value)
                        };
                    } catch (error) {
                        this.logger.error(`❌ Error decoding attribute:`, error);
                        return { key: '', value: '' };
                    }
                }).filter((attr: any) => attr.key && attr.value);

                if (attributes.length > 0) {
                    didEvents.push({
                        type: event.type,
                        txHash,
                        height,
                        attributes
                    });
                }
            }
        }

        return didEvents;
    }

    private getDIDFromEvent(event: DIDEvent): DIDDetails | null {
        if (event.type === 'touch_did' || event.type === 'remove_did') {
            const didAttr = event.attributes.find(a => a.value && a.value.startsWith('did:'));
            if (didAttr) {
                return { did: didAttr.value };
            }
            return null;
        }

        const didAttr = event.attributes.find(a =>
            ['did', 'DID', 'did_document.did'].includes(a.key) &&
            a.value &&
            a.value.startsWith('did:')
        );

        if (!didAttr) {
            this.logger.warn(`⚠️ No valid DID found in event attributes: ${JSON.stringify(event.attributes)}`);
            return null;
        }

        const details: DIDDetails = { did: didAttr.value };
        const attributeMap: Record<string, keyof DIDDetails> = {
            'controller': 'controller',
            'exp': 'exp',
            'deposit': 'deposit',
            'msg_index': 'msg_index'
        };

        for (const attr of event.attributes) {
            const mappedKey = attributeMap[attr.key];
            if (mappedKey && !details[mappedKey]) {
                (details as any)[mappedKey] = attr.value;
            }
        }

        this.logger.debug(`ℹ️ Extracted DID details: ${JSON.stringify(details)}`);
        return details;
    }

    private tryDecodeBase64(input: string): string {
        if (typeof input === 'string' && /^[a-zA-Z0-9_\-:.]+$/.test(input)) {
            return input;
        }

        try {
            const decoded = Buffer.from(input, 'base64').toString('utf8');
            if (decoded && !decoded.includes('�')) {
                return decoded;
            }
        } catch (e) {
        }

        return input;
    }


    private async processDIDEvents(events: DIDEvent[], height: number): Promise<void> {
        this.logger.info(`🔍 Processing ${events.length} DID events at height ${height}`);

        for (const event of events) {
            try {
                this.logger.info(`🔄 Processing ${event.type} event`);
                this.logger.warn(`📦 Event data: ${JSON.stringify(event)}`);

                const didDetails = this.getDIDFromEvent(event);
                if (!didDetails) {
                    this.logger.warn(`⚠️ Skipping event - no valid DID found`);
                    continue;
                }

                this.logger.info(`ℹ️ Processing DID: ${didDetails.did}`);

                switch (event.type) {
                    case 'add_did':
                        await this.handleAddDID(didDetails, height);
                        break;
                    case 'remove_did':
                        await this.handleRemoveDID(didDetails);
                        break;
                    case 'renew_did':
                    case 'touch_did':
                        await this.handleRenewDID(didDetails, height);
                        break;
                }
            } catch (error) {
                this.logger.error(`💥 Error processing DID event:`, error);
            }
        }
    }


    private async handleAddDID(details: DIDDetails, height: number): Promise<void> {
        const didData = {
            did: details.did,
            controller: details.controller,
            height: height,
            created: details?.created ? new Date(details.created) : new Date(),
            modified: details?.modified ? new Date(details.modified) : new Date(),
            exp: details?.exp ? new Date(details.exp) : new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
            deposit: details.deposit || '0',
        };
        await this.broker.call(`${'v1.DIDDatabaseService'}.upsert`, didData);
        this.logger.info(`✅ DID ${details.did} added successfully`);
    }



    private async handleRemoveDID(details: DIDDetails): Promise<void> {
        await this.broker.call(`${'v1.DIDDatabaseService'}.delete`, { did: details.did });
        this.logger.info(`✅ DID ${details.did} removed successfully`);
    }

    private async handleRenewDID(details: DIDDetails, height: number): Promise<void> {
        const did = details.did;
        if (!did) {
            this.logger.warn('⚠️ No DID provided in renew event.');
            return;
        }

        try {
            const apiUrl = `${config.veranaApi || 'https://api.testnet.verana.network'}/verana/dd/v1/get/${did}`;
            const response = await axios.get(apiUrl);
            const data = response?.data?.did_entry;
            this.logger.info(`📦 Received DID data: ${data}`);
            const didData: any = {
                did,
                height,
            };

            // Fetch existing record by DID
            const existingDid = await this.broker.call('v1.DIDDatabaseService.get', { did }) as {
                did?: string;
                controller?: string;
                created?: string;
                modified?: string;
                exp?: string;
                deposit?: string;
                height?: number;
            };
            if (existingDid) {
                this.logger.info(`ℹ️ Found existing DID record: ${JSON.stringify(existingDid)}`);
            }
            didData.did = data?.did || existingDid?.did || did;

            didData.controller = data?.controller || existingDid?.controller || null;

            didData.modified = data?.modified
                ? new Date(data.modified)
                : existingDid?.modified
                    ? new Date(existingDid.modified)
                    : new Date(); 

            didData.exp = data?.exp
                ? new Date(data.exp)
                : existingDid?.exp
                    ? new Date(existingDid.exp)
                    : new Date(Date.now() + 365 * 24 * 60 * 60 * 1000); 

            didData.deposit = data?.deposit || existingDid?.deposit || null;

            didData.created = data?.created
                ? new Date(data.created)
                : existingDid?.created
                    ? new Date(existingDid.created)
                    : new Date();

            await this.broker.call('v1.DIDDatabaseService.upsert', didData);
            this.logger.info(`✅ DID ${did} renewed and updated from chain`);
            } catch (err) {
            this.logger.error(`❌ Failed to fetch/renew DID ${did}:`, err);
        }
    }

    public async _stop(): Promise<void> {
        this.logger.info('🛑 Stopping DID Event Listener Service...');
        this.wsClient?.close();
        await super._stop();
    }
}