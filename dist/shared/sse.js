// A server-sent events parser over a fetch body, following the WHATWG event-stream rules: lines
// end with LF, CRLF or CR; comments (lines starting with ":") are ignored; `data` lines are joined
// with "\n"; a frame dispatches on a blank line if it had data; the last event ID persists across
// frames; `event` defaults to "message". EventSource can't send a POST body or an Authorization
// header, so the providers read streams with fetch and this.
export async function* readSSE(body) {
    const reader = body.pipeThrough(new TextDecoderStream()).getReader();
    // A line still waiting for its ending, in pieces: each chunk is scanned once, so a long line
    // (a whole snapshot is one `data:` line) costs linear time, not a rescan per chunk.
    let partial = [];
    let first = true;
    let skipLF = false; // the last chunk ended with a CR: an LF starting the next one is its pair
    let event = "";
    let data = [];
    let lastId;
    const ending = /[\r\n]/g;
    try {
        for (;;) {
            const { value, done } = await reader.read();
            if (done)
                return; // an unfinished line or frame at EOF dispatches nothing
            let chunk = value;
            if (first && chunk.length > 0) {
                if (chunk.charCodeAt(0) === 0xfeff)
                    chunk = chunk.slice(1);
                first = false;
            }
            let i = 0;
            if (skipLF && chunk.length > 0) {
                if (chunk.charCodeAt(0) === 10)
                    i = 1;
                skipLF = false;
            }
            for (;;) {
                ending.lastIndex = i;
                const m = ending.exec(chunk);
                if (!m) {
                    if (i < chunk.length)
                        partial.push(chunk.slice(i));
                    break;
                }
                const j = m.index;
                let line = chunk.slice(i, j);
                if (partial.length > 0) {
                    partial.push(line);
                    line = partial.join("");
                    partial = [];
                }
                i = j + 1;
                if (chunk.charCodeAt(j) === 13) {
                    if (i < chunk.length) {
                        if (chunk.charCodeAt(i) === 10)
                            i++;
                    }
                    else
                        skipLF = true;
                }
                if (line === "") {
                    if (data.length > 0) {
                        const ev = { event: event || "message", data: data.join("\n") };
                        if (lastId !== undefined)
                            ev.id = lastId;
                        yield ev;
                    }
                    event = "";
                    data = [];
                    continue;
                }
                if (line.charCodeAt(0) === 58)
                    continue; // ":" comment
                const colon = line.indexOf(":");
                const field = colon < 0 ? line : line.slice(0, colon);
                let val = colon < 0 ? "" : line.slice(colon + 1);
                if (val.charCodeAt(0) === 32)
                    val = val.slice(1);
                if (field === "event")
                    event = val;
                else if (field === "data")
                    data.push(val);
                else if (field === "id" && !val.includes("\0"))
                    lastId = val;
            }
        }
    }
    finally {
        await reader.cancel().catch(() => { });
        reader.releaseLock();
    }
}
/**
 * readSSE over a stream that must not go silent: any byte (a heartbeat comment included) counts
 * as activity, and after `idleMs` without any the controller is aborted, ending the read. Each
 * chunk also calls `onActivity`.
 */
export async function* watchedSSE(body, ctl, idleMs, onActivity = () => { }) {
    let last = Date.now();
    const idle = setInterval(() => {
        if (Date.now() - last > idleMs)
            ctl.abort();
    }, Math.max(10, Math.min(idleMs / 3, 5_000)));
    const watched = body.pipeThrough(new TransformStream({
        transform(chunk, c) {
            last = Date.now();
            onActivity();
            c.enqueue(chunk);
        },
    }));
    try {
        yield* readSSE(watched);
    }
    finally {
        clearInterval(idle);
    }
}
/** The reason of an `end` event (`{"reason": "rotate"}`), or "unknown". */
export function endReason(data) {
    try {
        const v = JSON.parse(data);
        return typeof v.reason === "string" ? v.reason : "unknown";
    }
    catch {
        return "unknown";
    }
}
/** Whether a response is an event stream (Content-Type text/event-stream). */
export function isEventStream(res) {
    return (res.headers.get("content-type") ?? "").toLowerCase().startsWith("text/event-stream");
}
