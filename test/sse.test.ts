import { expect, test } from "bun:test"
import { readSSE } from "../src/shared/sse.ts"

function streamOf(...chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder()
  return new ReadableStream({
    start(c) {
      for (const ch of chunks) c.enqueue(enc.encode(ch))
      c.close()
    },
  })
}

async function all(body: ReadableStream<Uint8Array>) {
  const out = []
  for await (const ev of readSSE(body)) out.push(ev)
  return out
}

test("frames split across chunks", async () => {
  expect(await all(streamOf("event: snap", "shot\nid: 1\nda", "ta: {\"a\":1}\n", "\nevent: end\ndata: x\n\n"))).toEqual([
    { event: "snapshot", id: "1", data: '{"a":1}' },
    { event: "end", id: "1", data: "x" },
  ])
})

test("a multi-byte character split across chunks", async () => {
  const bytes = new TextEncoder().encode("data: é\n\n")
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(bytes.slice(0, 7))
      c.enqueue(bytes.slice(7))
      c.close()
    },
  })
  expect(await all(body)).toEqual([{ event: "message", data: "é" }])
})

test("comments ignored", async () => {
  expect(await all(streamOf(": open\n\n: heartbeat\n\nevent: changed\n: inline\ndata: {}\n\n"))).toEqual([
    { event: "changed", data: "{}" },
  ])
})

test("id and event carried, defaults to message", async () => {
  expect(await all(streamOf("id: 7\ndata: a\n\ndata: b\n\n"))).toEqual([
    { event: "message", id: "7", data: "a" },
    { event: "message", id: "7", data: "b" },
  ])
})

test("multi-line data joined with a newline", async () => {
  expect(await all(streamOf("data: one\ndata: two\ndata:three\n\n"))).toEqual([{ event: "message", data: "one\ntwo\nthree" }])
})

test("CRLF and CR line endings", async () => {
  expect(await all(streamOf("event: a\r\ndata: 1\r\n\r\nevent: b\rdata: 2\r\r"))).toEqual([
    { event: "a", data: "1" },
    { event: "b", data: "2" },
  ])
})

test("a CRLF split across chunks is one line ending", async () => {
  expect(await all(streamOf("event: a\r", "\ndata: 1\r", "\n\r", "\n"))).toEqual([{ event: "a", data: "1" }])
})

test("a frame without data, and an unfinished frame at EOF, dispatch nothing", async () => {
  expect(await all(streamOf("event: x\n\ndata: done\n\nevent: half\ndata: y"))).toEqual([{ event: "message", data: "done" }])
})

test("a field without a colon, and a leading BOM", async () => {
  expect(await all(streamOf("﻿data\n\nretry: 10\ndata: x\n\n"))).toEqual([
    { event: "message", data: "" },
    { event: "message", data: "x" },
  ])
})

test("an 8 MB line in 16 KB chunks parses in linear time", async () => {
  const size = 8 << 20
  const bytes = new TextEncoder().encode(`event: snapshot\nid: 9\ndata: ${"x".repeat(size)}\n\n`)
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      for (let i = 0; i < bytes.length; i += 16 << 10) c.enqueue(bytes.subarray(i, i + (16 << 10)))
      c.close()
    },
  })
  const t0 = performance.now()
  const evs = await all(body)
  const ms = performance.now() - t0
  expect(evs.length).toBe(1)
  expect(evs[0]!.data.length).toBe(size)
  expect(ms).toBeLessThan(200)
})
