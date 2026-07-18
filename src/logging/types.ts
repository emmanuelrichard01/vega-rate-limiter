export interface LogEntry {
  clientId: string;
  allowed: boolean;
  latencyMs: number;
  source: 'redis' | 'fallback';
  occurredAt: Date;
}
