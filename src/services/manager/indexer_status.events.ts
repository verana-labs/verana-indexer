export type IndexerStatusEvent = {
  indexer_status: "running" | "stopped";
  crawling_status: "active" | "stopped";
  stopped_at?: string;
  stopped_reason?: string;
  last_error?: {
    message: string;
    timestamp: string;
    service?: string;
  };
};

