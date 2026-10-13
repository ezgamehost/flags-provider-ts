import type { EvaluationContext, JsonValue, Logger, ResolutionDetails } from "@openfeature/core";
export type FlagType = "boolean" | "string" | "number" | "object";
export declare class Evaluator {
    #private;
    /** The snapshot's `metadata.snapshotVersion`. */
    readonly version: number;
    /**
     * Loads a snapshot (flagd JSON as flags-data serves it). Throws on an invalid snapshot. The
     * core runs without dynamic code generation (it works in every runtime, edge ones included);
     * `dynamicCodeGeneration: true` uses the core's compiled mode, which the conformance suite also
     * runs.
     */
    constructor(snapshotJson: string, logger?: Logger, options?: {
        dynamicCodeGeneration?: boolean;
    });
    /**
     * A fingerprint of each flag's definition (variants, default, targeting with the segments it
     * references resolved, version, enabled), by key. Two snapshots' fingerprints differ for a flag
     * exactly when its answers may differ, so a segment edit, which changes no flagVersion, still
     * shows up in `changedKeys`.
     */
    fingerprints(): Map<string, string>;
    /** Each flag's `metadata.flagVersion` (its config version), by key. */
    flagVersions(): Map<string, number>;
    /** The snapshot's flag keys, sorted. */
    keys(): string[];
    resolve<T extends JsonValue>(type: FlagType, key: string, defaultValue: T, ctx: EvaluationContext): ResolutionDetails<T>;
}
/**
 * The keys whose value changed, appeared or disappeared between two maps (`flagVersions()` or
 * `fingerprints()`), sorted.
 */
export declare function changedKeys<V>(before: Map<string, V>, after: Map<string, V>): string[];
