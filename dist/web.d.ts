import { OpenFeatureEventEmitter, type EvaluationContext, type JsonValue, type Logger, type Provider, type ResolutionDetails } from "@openfeature/web-sdk";
export { DEFAULT_ENDPOINT } from "./shared/http.ts";
export interface EzghWebProviderOptions {
    /** A client SDK key (`ezgh_fc_…`). It's publishable: it sees only `clientSide` flags. */
    clientKey: string;
    /** The data plane; defaults to https://flags.ezghcloud.com. */
    endpoint?: string;
    /** Follow the client change stream and refetch on every change (default false: one connection per open tab). */
    live?: boolean;
    /** The periodic refresh interval (default 5 minutes). */
    refreshMs?: number;
    /** The fetch to use; defaults to the global one. */
    fetch?: typeof fetch;
    /** Logs connection problems; defaults to console warnings and errors. */
    logger?: Logger;
    /** How long `initialize` retries the first evaluation (default 10 s). Retrying continues after. */
    timeoutMs?: number;
    /** Retry backoff bounds (default 1 s to 60 s, with jitter). */
    minBackoffMs?: number;
    maxBackoffMs?: number;
    /** A live stream silent this long (heartbeats come every 15 s) is reconnected (default 45 s). */
    idleTimeoutMs?: number;
    /**
     * After a `changed` whose refetch answers 304 (the pod answering may not have the new version
     * yet), refetch once more after this long (default 1 s).
     */
    changedRetryMs?: number;
}
export declare class EzghWebProvider implements Provider {
    #private;
    readonly metadata: {
        readonly name: "ezgh-flags";
    };
    readonly runsOn: "client";
    readonly events: OpenFeatureEventEmitter;
    constructor(options: EzghWebProviderOptions);
    initialize(context?: EvaluationContext): Promise<void>;
    onContextChange(_old: EvaluationContext, next: EvaluationContext): Promise<void>;
    onClose(): Promise<void>;
    resolveBooleanEvaluation(flagKey: string, defaultValue: boolean): ResolutionDetails<boolean>;
    resolveStringEvaluation(flagKey: string, defaultValue: string): ResolutionDetails<string>;
    resolveNumberEvaluation(flagKey: string, defaultValue: number): ResolutionDetails<number>;
    resolveObjectEvaluation<T extends JsonValue>(flagKey: string, defaultValue: T): ResolutionDetails<T>;
}
