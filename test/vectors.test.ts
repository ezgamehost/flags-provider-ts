// Every flags-stack conformance vector (test/vectors, synced verbatim by scripts/sync-vectors.sh)
// through the shared Evaluator, comparing the five fields protocol/README.md names.
import { describe, expect, test } from "bun:test"
import { readdirSync, readFileSync } from "node:fs"
import { Evaluator } from "../src/shared/evaluator.ts"

const dir = new URL("./vectors/", import.meta.url)
const files = readdirSync(dir).filter((f) => f.endsWith(".json"))

test("vectors are present", () => {
  expect(files.length).toBeGreaterThan(0)
})

for (const dynamicCodeGeneration of [false, true])
for (const file of files) {
  const v = JSON.parse(readFileSync(new URL(file, dir), "utf8"))
  describe(`${v.name}${dynamicCodeGeneration ? " (dynamic code generation)" : ""}`, () => {
    const ev = new Evaluator(JSON.stringify(v.snapshot), undefined, { dynamicCodeGeneration })
    for (const [i, c] of v.cases.entries()) {
      test(`${i} ${c.flag} ${JSON.stringify(c.context)}`, () => {
        const r = ev.resolve(c.type, c.flag, c.default, c.context)
        expect(r.value).toEqual(c.expect.value)
        if (c.expect.errorCode) {
          expect(r.errorCode).toBe(c.expect.errorCode)
        } else {
          expect(r.errorCode).toBeUndefined()
          expect(r.variant).toBe(c.expect.variant)
          expect(r.reason).toBe(c.expect.reason)
          expect(r.flagMetadata?.flagVersion).toBe(c.expect.flagVersion)
        }
      })
    }
  })
}
