import { AttemptsCycleRunner } from './cycle-runner.js';
import { LargeBatchCoordinator } from '../../large-batch/coordinator.js';
import type { LargePlan, RawSource } from '../../large-batch/types.js';

export interface AttemptsCycleDependencies {
  coordinator: LargeBatchCoordinator;
  plan: () => LargePlan;
  source: () => RawSource;
  onSuccess?: (result: {
    received: number;
    persisted: number;
    duplicates: number;
    rejected: number;
  }) => void;
}

export function createAttemptsRecurringRunner(
  dependencies: AttemptsCycleDependencies,
  intervalMs = 60_000,
): AttemptsCycleRunner {
  return new AttemptsCycleRunner(async signal => {
    const plan = dependencies.plan();
    const source = dependencies.source();

    const result = await dependencies.coordinator.receive(
      plan,
      source,
      signal,
    );

    dependencies.onSuccess?.(result);
  }, intervalMs);
}
