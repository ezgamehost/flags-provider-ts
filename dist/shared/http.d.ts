export declare const DEFAULT_ENDPOINT = "https://flags.ezghcloud.com";
export declare function trimEndpoint(endpoint: string | undefined): string;
/** A Retry-After header (seconds or an HTTP date) in milliseconds, or undefined. */
export declare function retryAfterMs(res: Response, now?: number): number | undefined;
/**
 * Exponential backoff with jitter: attempt 1 waits between min/2 and min, doubling up to max, and
 * never less than the server's Retry-After, itself capped at max (a broken or hostile Retry-After
 * can't park a provider for hours).
 */
export declare function backoffMs(attempt: number, minMs: number, maxMs: number, retryAfter?: number): number;
/** Resolves after ms, or at once when the signal aborts. */
export declare function sleep(ms: number, signal: AbortSignal): Promise<void>;
/** An AbortController that also aborts when the parent signal does. */
export declare function childController(parent: AbortSignal): AbortController;
/** Whether a status means the key itself is refused (unknown, revoked, or the wrong kind). */
export declare function isFatalStatus(status: number): boolean;
