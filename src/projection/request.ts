import { digest } from '../persistence/validation.js';
import type { Candidate } from './contract.js';
export function requestDigest(row: Readonly<Candidate>, destinationId: string, contractVersion: string, mappingVersion: string): string {
  return digest({ row,destinationId,contractVersion,mappingVersion });
}
