export interface SSEEvent {
    event: string;
    id?: string;
    data: string;
}
export declare function readSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<SSEEvent>;
/**
 * readSSE over a stream that must not go silent: any byte (a heartbeat comment included) counts
 * as activity, and after `idleMs` without any the controller is aborted, ending the read. Each
 * chunk also calls `onActivity`.
 */
export declare function watchedSSE(body: ReadableStream<Uint8Array>, ctl: AbortController, idleMs: number, onActivity?: () => void): AsyncGenerator<SSEEvent>;
/** The reason of an `end` event (`{"reason": "rotate"}`), or "unknown". */
export declare function endReason(data: string): string;
/** Whether a response is an event stream (Content-Type text/event-stream). */
export declare function isEventStream(res: Response): boolean;
