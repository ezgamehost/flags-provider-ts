# @ezgh/flags-openfeature

[OpenFeature](https://openfeature.dev) providers for **EZGH Feature Flags**, in TypeScript:

- `@ezgh/flags-openfeature/server`: `EzghProvider`, for backends on Node (20+), Bun and Deno.
  It keeps the environment's compiled flags in memory and evaluates locally with flagd's
  open-source core, so an evaluation never makes a network call.
- `@ezgh/flags-openfeature/web`: `EzghWebProvider`, for browsers (React through OpenFeature's
  React SDK). It evaluates remotely, so rules never reach the browser.

You write against the OpenFeature API; the provider connects it to EZGH. Licensed Apache-2.0.

## Install

The package installs from this repository's release tags, which carry the built `dist/`
(it isn't published to npm):

```bash
npm i github:ezgamehost/flags-provider-ts#v0.1.0
bun add github:ezgamehost/flags-provider-ts#v0.1.0
pnpm add github:ezgamehost/flags-provider-ts#v0.1.0
```

Install the OpenFeature SDK you use alongside it: `@openfeature/server-sdk` for the server
provider, `@openfeature/web-sdk` (and `@openfeature/react-sdk` for React) for the web provider.

## Server provider

Create a **server key** (`ezgh_fs_…`) for an environment in the console or with
`ezgh flags sdk-keys create`, and keep it secret: it can read every flag's rules.

```ts
import { OpenFeature } from "@openfeature/server-sdk"
import { EzghProvider } from "@ezgh/flags-openfeature/server"

await OpenFeature.setProviderAndWait(new EzghProvider({ sdkKey: process.env.EZGH_FLAGS_KEY! }))
const flags = OpenFeature.getClient()

if (await flags.getBooleanValue("new-checkout", false, { targetingKey: user.id, plan: user.plan })) {
  // …
}
```

- **Startup:** the provider fetches the environment's snapshot and is ready once it has it, then
  follows the data plane's sync stream. If the stream fails three times in a row, it polls the
  snapshot every 30 seconds while it keeps retrying the stream (exponential backoff with jitter,
  honouring `Retry-After`).
- **Changes:** every new snapshot version emits `PROVIDER_CONFIGURATION_CHANGED` with
  `metadata.snapshotVersion` and `flagsChanged`: the flags whose configuration changed, appeared
  or disappeared (a segment edit counts for the flags using it; the list can be empty).
- **Disconnected** for 30 seconds: it keeps evaluating the last snapshot and emits
  `PROVIDER_STALE`, then `PROVIDER_READY` once it reaches the data plane again.
- **A revoked or unknown key** is fatal (a stream ending with `revoked` or `environmentDeleted`,
  or a `401`/`403`):
  `PROVIDER_ERROR` with `PROVIDER_FATAL`, the client status becomes `FATAL` (OpenFeature then
  answers defaults), and the provider stops.
- **Reasons:** `TARGETING_MATCH`, `STATIC`, `DEFAULT`, and `DISABLED` for a flag that's off in
  the environment (with its off value). `flagMetadata.flagVersion` is the flag's config version.

### Options

| Option | Default | |
| --- | --- | --- |
| `sdkKey` | | A server key. Required unless `offline`. |
| `endpoint` | `https://flags.ezghcloud.com` | The data plane. |
| `bootstrapPath` | | A snapshot file from `ezgh flags export`. The provider is ready from it without the network, then syncs; the data plane's snapshot replaces it whatever its version. |
| `offline` | `false` | Use only `bootstrapPath` and never call the network (tests, CI, air-gapped runs). |
| `timeoutMs` | `10000` | How long `initialize` waits for the first snapshot. On a timeout it fails (status `ERROR`) and keeps trying; it emits `PROVIDER_READY` when the snapshot arrives. |
| `pollIntervalMs` | `30000` | The snapshot poll interval while the stream is down. |
| `staleAfterMs` | `30000` | How long without reaching the data plane before `PROVIDER_STALE`. |
| `minBackoffMs`, `maxBackoffMs` | `1000`, `60000` | Stream reconnect backoff. A `Retry-After` is honoured up to the maximum. A stream ending with `rotate` reconnects at once; `shutdown` after a little jitter; any other reason backs off like a failure. |
| `idleTimeoutMs` | `45000` | A stream silent this long (the data plane sends a heartbeat every 15 s) is reconnected. |
| `fetch`, `logger` | global `fetch`, console | |

### Offline and tests

```ts
// A file from `ezgh flags export --env production > flags.json`; no network at all.
await OpenFeature.setProviderAndWait(new EzghProvider({ offline: true, bootstrapPath: "flags.json" }))
```

## Browsers and React

Create a **client key** (`ezgh_fc_…`) for the environment. It's publishable: ship it in your
bundle. A client key only sees flags marked **client-side** (`clientSide: true`); any other flag
answers `FLAG_NOT_FOUND` exactly as a flag that doesn't exist, and rules never leave EZGH.

```ts
import { OpenFeature } from "@openfeature/web-sdk"
import { EzghWebProvider } from "@ezgh/flags-openfeature/web"

await OpenFeature.setContext({ targetingKey: user.id, plan: user.plan })
OpenFeature.setProvider(new EzghWebProvider({ clientKey: "ezgh_fc_…" }))
```

- **Evaluation:** one bulk evaluation of every client-side flag at startup and on every
  `OpenFeature.setContext`; flags are then answered synchronously from those results.
- **Refresh:** when the page becomes visible again and every 5 minutes (`refreshMs`), with
  `If-None-Match`, so an unchanged answer costs a `304`. Changes emit
  `PROVIDER_CONFIGURATION_CHANGED` with the flags whose value or variant changed.
- **Live updates** are opt-in: `new EzghWebProvider({ clientKey, live: true })` keeps one stream
  per tab open and refetches as soon as a client-side flag changes.
- Nothing is stored in `localStorage` or `sessionStorage`; results live in memory only.
- A revoked key or a deleted environment is fatal (`PROVIDER_FATAL`), as for the server provider.

With React, use OpenFeature's React SDK; there's no EZGH React package:

```tsx
import { OpenFeatureProvider, useBooleanFlagValue } from "@openfeature/react-sdk"

function App() {
  return (
    <OpenFeatureProvider>
      <Checkout />
    </OpenFeatureProvider>
  )
}

function Checkout() {
  const newCheckout = useBooleanFlagValue("new-checkout", false)
  return newCheckout ? <NewCheckout /> : <OldCheckout />
}
```

Options: `clientKey`, `endpoint`, `live` (`false`), `refreshMs` (5 minutes), `timeoutMs` (10 s for
the first evaluation; it keeps retrying after and emits `PROVIDER_READY`), `minBackoffMs`,
`maxBackoffMs`, `idleTimeoutMs`, `changedRetryMs` (1 s: after a `changed` whose refetch is a `304`,
one more refetch this much later), `fetch`, `logger`. A context the data plane refuses (`400
INVALID_CONTEXT`) isn't retried; the provider answers defaults until the context changes. Each
live-stream connection refreshes once (conditionally), the first included.

## Conformance

`test/vectors/` holds flags-stack's conformance vectors (`protocol/vectors/eval`) verbatim, at the
commit in `.flags-stack-rev`. Every case is evaluated through this package's evaluator and must
match the value, variant, reason, flag version and error code that flags-stack, the Go provider
and the Python provider answer. CI checks the copy against flags-stack with a read-only deploy key.

To update them: `scripts/sync-vectors.sh ~/code/flags-stack <commit>`, then `bun run check`. Don't
edit the vectors here; a failing vector is a cross-core disagreement to fix in flags-stack.

## Development

```bash
bun install
bun run check   # typecheck, tests (vectors included), build, a Node import smoke test
```

**End to end:** `scripts/e2e.sh <flags-stack checkout>` builds flags-stack and its fake org-api,
runs `flags-stack all` on `DATABASE_URL` (a database of its own) and `REDIS_URL`, and runs
`test/e2e/e2e.test.ts`: an environment, a client-side flag and both kinds of key through the API;
both providers see it off, then on within seconds of a config write; revoking the server key makes
its provider fatal. CI runs it in the E2E workflow against flags-stack at `.flags-stack-rev`.

**Releases:** the Release workflow (manual, with a version) runs the checks, builds, and pushes tag
`v<version>` on a detached commit that adds `dist/` and sets `package.json`'s version. `main`
never holds `dist/`.
