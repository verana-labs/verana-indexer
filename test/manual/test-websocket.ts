/* eslint-disable no-console */

import WebSocket from 'ws';

interface BlockIndexedEvent {
  type: 'block-indexed';
  height: number;
  timestamp: string;
}

interface BlockResolvedEvent {
  type: 'block-resolved';
  height: number;
  timestamp: string;
}

interface ConnectedEvent {
  type: 'connected';
  message: string;
}

type EventMessage = BlockIndexedEvent | BlockResolvedEvent | ConnectedEvent;

const ws = new WebSocket('ws://localhost:3001/verana/indexer/v1/events');

ws.on('open', () => {
  console.log('✅ Connected to Verana Indexer Events WebSocket');
  console.log('Waiting for block-indexed / block-resolved events...\n');
});

ws.on('message', (data: WebSocket.Data) => {
  try {
    const event: EventMessage = JSON.parse(data.toString()) as EventMessage;
    console.log('📦 Received event:', JSON.stringify(event, null, 2));
    
    if (event.type === 'block-indexed') {
      console.log(`\n Block indexed (tx pipeline). Height: ${event.height}, Time: ${event.timestamp}\n`);
    }
    if (event.type === 'block-resolved') {
      console.log(`\n Block trust-resolved. Height: ${event.height}, Time: ${event.timestamp}\n`);
    }
  } catch (error) {
    console.error('❌ Error parsing message:', error);
    console.log('Raw message:', data.toString());
  }
});

ws.on('error', (err: Error) => {
  console.error('❌ WebSocket error:', err.message);
  console.log('\n💡 Make sure:');
  console.log('   1. Indexer is running (npm run dev)');
  console.log('   2. Server is on port 3001');
  console.log('   3. WebSocket server is initialized\n');
});

ws.on('close', () => {
  console.log('\n🔌 WebSocket connection closed');
});

process.on('SIGINT', () => {
  console.log('\n👋 Closing connection...');
  ws.close();
  process.exit(0);
});

