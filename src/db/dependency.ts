export type DependencyName = 'olos' | 'postgres';
export type DependencyState = 'unconfigured' | 'unknown' | 'up' | 'down' | 'stopped';
export interface DependencyStatus {
  state: DependencyState;
  checkedAt: string | null;
  consecutiveFailures: number;
}
export interface Dependency {
  readonly name: DependencyName;
  readonly configured: boolean;
  probe(): Promise<void>;
  close(): Promise<void>;
}
