import { EventsBroadcaster } from "../../../../src/services/api/events_broadcaster";
import { createServer, Server } from "http";
import { WebSocket } from "ws";

describe("EventsBroadcaster", () => {
  let broadcaster: EventsBroadcaster;
  let httpServer: Server;
  const TEST_PORT = 9999;
  const WS_URL = `ws://localhost:${TEST_PORT}/verana/indexer/v1/events`;

  beforeAll(() => {
    broadcaster = new EventsBroadcaster();
    httpServer = createServer();
    broadcaster.initialize(httpServer);
    httpServer.listen(TEST_PORT);
  });

  afterAll((done) => {
    broadcaster.close();
    setTimeout(() => {
      httpServer.close(() => {
        done();
      });
    }, 100);
  });

  beforeEach((done) => {
    if (broadcaster) {
      broadcaster.close();
    }
    setTimeout(() => {
      broadcaster = new EventsBroadcaster();
      broadcaster.initialize(httpServer);
      setTimeout(() => {
        done();
      }, 150);
    }, 200);
  });

  describe("WebSocket Connection", () => {
    it("should accept WebSocket connections", (done) => {
      const ws = new WebSocket(WS_URL);

      ws.on("open", () => {
        expect(broadcaster.getWSClientCount()).toBe(1);
        ws.close();
      });

      ws.on("close", () => {
        setTimeout(() => {
          expect(broadcaster.getWSClientCount()).toBe(0);
          done();
        }, 100);
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);

    it("should send welcome message on connection", (done) => {
      const ws = new WebSocket(WS_URL);
      let welcomeReceived = false;

      ws.on("open", () => {
        setTimeout(() => {
          if (!welcomeReceived) {
            ws.close();
            done(new Error("Welcome message not received"));
          }
        }, 1000);
      });

      ws.on("message", (data) => {
        try {
          const message = JSON.parse(data.toString());
          expect(message.type).toBe("connected");
          expect(message.message).toBe("Connected to Verana Indexer Events");
          welcomeReceived = true;
          ws.close();
        } catch (error) {
          done(error);
        }
      });

      ws.on("close", () => {
        if (welcomeReceived) {
          done();
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);

    it("should handle multiple concurrent connections", (done) => {
      const clients: WebSocket[] = [];
      const clientCount = 3;
      let connectedCount = 0;

      for (let i = 0; i < clientCount; i++) {
        const ws = new WebSocket(WS_URL);
        clients.push(ws);

        ws.on("open", () => {
          connectedCount++;
          if (connectedCount === clientCount) {
            expect(broadcaster.getWSClientCount()).toBe(clientCount);
            clients.forEach((client) => client.close());
          }
        });

        ws.on("close", () => {
          if (clients.every((c) => c.readyState === WebSocket.CLOSED)) {
            setTimeout(() => {
              expect(broadcaster.getWSClientCount()).toBe(0);
              done();
            }, 200);
          }
        });

        ws.on("error", (error) => {
          done(error);
        });
      }
    }, 15000);
  });

  describe("Block Processed Events", () => {
    it("should broadcast block-processed events to all connected clients", (done) => {
      const ws1 = new WebSocket(WS_URL);
      const ws2 = new WebSocket(WS_URL);
      const receivedMessages: any[] = [];
      let bothConnected = false;

      const checkDone = () => {
        if (bothConnected && receivedMessages.length === 2) {
          receivedMessages.forEach((msg) => {
            expect(msg.type).toBe("block-processed");
            expect(msg.height).toBe(123456);
            expect(msg.timestamp).toBeDefined();
            expect(msg.version).toBeDefined();
            expect(msg.version).toMatch(/^v\d+\.\d+\.\d+/);
          });
          ws1.close();
          ws2.close();
          done();
        }
      };

      ws1.on("open", () => {
        ws2.on("open", () => {
          bothConnected = true;
          broadcaster.broadcastBlockProcessed(123456, new Date());
        });
      });

      ws1.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-processed") {
          receivedMessages.push(message);
          checkDone();
        }
      });

      ws2.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-processed") {
          receivedMessages.push(message);
          checkDone();
        }
      });

      ws1.on("error", (error) => done(error));
      ws2.on("error", (error) => done(error));
    }, 10000);

    it("should format timestamp correctly", (done) => {
      const ws = new WebSocket(WS_URL);
      const testTimestamp = new Date("2025-01-15T10:30:00.000Z");
      const expectedFormat = "2025-01-15T10:30:00Z";
      let blockProcessedReceived = false;

      ws.on("open", () => {
        broadcaster.broadcastBlockProcessed(789012, testTimestamp);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-processed") {
          expect(message.timestamp).toBe(expectedFormat);
          expect(new Date(message.timestamp).getTime()).toBe(testTimestamp.getTime());
          expect(message.version).toBeDefined();
          expect(message.version).toMatch(/^v\d+\.\d+\.\d+/);
          blockProcessedReceived = true;
          ws.close();
        }
      });

      ws.on("close", () => {
        if (blockProcessedReceived) {
          done();
        } else {
          done(new Error("Block processed message not received"));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);

    it("should handle string timestamps", (done) => {
      const ws = new WebSocket(WS_URL);
      const testTimestamp = "2025-01-15T10:30:00.000Z";
      const expectedFormat = "2025-01-15T10:30:00Z";
      let blockProcessedReceived = false;

      ws.on("open", () => {
        broadcaster.broadcastBlockProcessed(345678, testTimestamp);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-processed") {
          expect(message.timestamp).toBe(expectedFormat);
          blockProcessedReceived = true;
          ws.close();
        }
      });

      ws.on("close", () => {
        if (blockProcessedReceived) {
          done();
        } else {
          done(new Error("Block processed message not received"));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);
  });

  describe("Client Management", () => {
    it("should track client count correctly", (done) => {
      expect(broadcaster.getWSClientCount()).toBe(0);

      const ws1 = new WebSocket(WS_URL);
      const ws2 = new WebSocket(WS_URL);
      let ws1Open = false;
      let ws2Open = false;
      let ws1Closed = false;
      let ws2Closed = false;

      const checkBothOpen = () => {
        if (ws1Open && ws2Open) {
          setTimeout(() => {
            const count = broadcaster.getWSClientCount();
            expect(count).toBe(2);
            ws1.close();
            ws2.close();
          }, 100);
        }
      };

      const checkBothClosed = () => {
        if (ws1Closed && ws2Closed) {
          setTimeout(() => {
            expect(broadcaster.getWSClientCount()).toBe(0);
            done();
          }, 200);
        }
      };

      ws1.on("open", () => {
        ws1Open = true;
        setTimeout(() => {
          const count = broadcaster.getWSClientCount();
          expect(count).toBeGreaterThanOrEqual(1);
        }, 50);
        checkBothOpen();
      });

      ws2.on("open", () => {
        ws2Open = true;
        checkBothOpen();
      });

      ws1.on("close", () => {
        ws1Closed = true;
        checkBothClosed();
      });

      ws2.on("close", () => {
        ws2Closed = true;
        checkBothClosed();
      });

      ws1.on("error", (error) => done(error));
      ws2.on("error", (error) => done(error));
    }, 15000);

    it("should cleanup disconnected clients", (done) => {
      const ws = new WebSocket(WS_URL);

      ws.on("open", () => {
        expect(broadcaster.getWSClientCount()).toBe(1);
        ws.close();
      });

      ws.on("close", () => {
        setTimeout(() => {
          expect(broadcaster.getWSClientCount()).toBe(0);
          done();
        }, 200);
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);
  });

  describe("Real-time Event Flow", () => {
    it("should receive multiple block-processed events in sequence", (done) => {
      const ws = new WebSocket(WS_URL);
      const receivedHeights: number[] = [];
      const expectedHeights = [100, 101, 102];

      ws.on("open", () => {
        setTimeout(() => broadcaster.broadcastBlockProcessed(100, new Date()), 100);
        setTimeout(() => broadcaster.broadcastBlockProcessed(101, new Date()), 200);
        setTimeout(() => broadcaster.broadcastBlockProcessed(102, new Date()), 300);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-processed") {
          receivedHeights.push(message.height);
          if (receivedHeights.length === expectedHeights.length) {
            expect(receivedHeights).toEqual(expectedHeights);
            ws.close();
          }
        }
      });

      ws.on("close", () => {
        if (receivedHeights.length === expectedHeights.length) {
          done();
        } else {
          done(new Error(`Expected ${expectedHeights.length} events, got ${receivedHeights.length}`));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);
  });

  describe("Error Handling", () => {
    it("should handle client errors gracefully", (done) => {
      const ws = new WebSocket(WS_URL);

      ws.on("open", () => {
        expect(broadcaster.getWSClientCount()).toBe(1);
        ws.terminate();
      });

      ws.on("error", () => {
      });

      setTimeout(() => {
        expect(broadcaster.getWSClientCount()).toBe(0);
        done();
      }, 500);
    }, 10000);
  });
});

