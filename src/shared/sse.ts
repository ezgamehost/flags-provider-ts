// A server-sent events parser over a fetch body, following the WHATWG event-stream rules: lines
// end with LF, CRLF or CR; comments (lines starting with ":") are ignored; `data` lines are joined
// with "\n"; a frame dispatches on a blank line if it had data; the last event ID persists across
// frames; `event` defaults to "message". EventSource can't send a POST body or an Authorization
// header, so the providers read streams with fetch and this.

export interface SSEEvent {
  event: string
  id?: string
  data: string
}

export async function* readSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<SSEEvent> {
  const reader = body.pipeThrough(new TextDecoderStream() as unknown as ReadableWritablePair<string, Uint8Array>).getReader()
  let buf = ""
  let first = true
  let event = ""
  let data: string[] = []
  let lastId: string | undefined
  let eof = false
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) {
        if (!buf.endsWith("\r")) return
        eof = true // a CR that ended the stream is a line ending
      } else buf += value
      if (first && buf.length > 0) {
        if (buf.charCodeAt(0) === 0xfeff) buf = buf.slice(1)
        first = false
      }
      let start = 0
      for (;;) {
        let end = -1
        for (let i = start; i < buf.length; i++) {
          const ch = buf.charCodeAt(i)
          if (ch === 10 || ch === 13) {
            end = i
            break
          }
        }
        if (end < 0) break
        // A CR at the very end of the buffer may be the first half of a CRLF: wait for more.
        if (buf.charCodeAt(end) === 13 && end === buf.length - 1 && !eof) break
        const line = buf.slice(start, end)
        start = end + (buf.charCodeAt(end) === 13 && buf.charCodeAt(end + 1) === 10 ? 2 : 1)
        if (line === "") {
          if (data.length > 0) {
            const ev: SSEEvent = { event: event || "message", data: data.join("\n") }
            if (lastId !== undefined) ev.id = lastId
            yield ev
          }
          event = ""
          data = []
          continue
        }
        if (line.charCodeAt(0) === 58) continue // ":" comment
        const colon = line.indexOf(":")
        const field = colon < 0 ? line : line.slice(0, colon)
        let val = colon < 0 ? "" : line.slice(colon + 1)
        if (val.charCodeAt(0) === 32) val = val.slice(1)
        if (field === "event") event = val
        else if (field === "data") data.push(val)
        else if (field === "id" && !val.includes("\0")) lastId = val
      }
      buf = buf.slice(start)
      if (eof) return
    }
  } finally {
    await reader.cancel().catch(() => {})
    reader.releaseLock()
  }
}
