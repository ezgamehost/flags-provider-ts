import { OpenFeatureEventEmitter, type EvaluationContext, type JsonValue, type Logger, type Provider, type ResolutionDetails } from "@openfeature/server-sdk";
export { DEFAULT_ENDPOINT } from "./shared/http.ts";
export interface EzghProviderOptions {
    /** A server SDK key (`ezgh_fs_…`). Required unless `offline`. */
    sdkKey?: string;
    /** The data plane; defaults to https://flags.ezghcloud.com. */
    endpoint?: string;
    /** A snapshot file (`ezgh flags export`): the provider is ready from it without the network. */
    bootstrapPath?: string;
    /** Use only `bootstrapPath`; never call the network (tests, CI, air-gapped runs). */
    offline?: boolean;
    /** The fetch to use; defaults to the global one. */
    fetch?: typeof fetch;
    /** Logs snapshot and connection problems; defaults to console warnings and errors. */
    logger?: Logger;
    /** How long `initialize` waits for the first snapshot (default 10 s). Syncing continues after. */
    timeoutMs?: number;
    /** The snapshot poll interval while the stream is down (default 30 s). */
    pollIntervalMs?: number;
    /** Stale after this long without reaching the data plane (default 30 s). */
    staleAfterMs?: number;
    /** Stream reconnect backoff bounds (default 1 s to 60 s, with jitter). */
    minBackoffMs?: number;
    maxBackoffMs?: number;
    /** A stream that sends nothing (not even a heartbeat) for this long is reconnected (default 45 s). */
    idleTimeoutMs?: number;
}
export declare class EzghProvider implements Provider {
    #private;
    readonly metadata: {
        readonly name: "ezgh-flags";
    };
    readonly runsOn: "server";
    readonly events: OpenFeatureEventEmitter;
    constructor(options: EzghProviderOptions);
    initialize(): Promise<void>;
    onClose(): Promise<void>;
    resolveBooleanEvaluation(flagKey: string, defaultValue: boolean, context: EvaluationContext): Promise<ResolutionDetails<boolean>>;
    resolveStringEvaluation(flagKey: string, defaultValue: string, context: EvaluationContext): Promise<ResolutionDetails<string>>;
    resolveNumberEvaluation(flagKey: string, defaultValue: number, context: EvaluationContext): Promise<ResolutionDetails<number>>;
    resolveObjectEvaluation<T extends JsonValue>(flagKey: string, defaultValue: T, context: EvaluationContext): Promise<ResolutionDetails<T>>;
}
