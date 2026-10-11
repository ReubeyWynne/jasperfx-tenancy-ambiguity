# Single-tenanted event stores and the session tenant

A minimal repro for how Marten, Polecat and Fisher handle an append to an existing stream from
a session opened with a tenant id when `Events.TenancyStyle` is `Single`, so the store has no
tenant dimension for events.

## Scenario

The same two tests run against each store, with default options apart from the connection:

1. An untenanted session starts a stream (`StartStream<Tally>`).
2. A session opened with `LightweightSession("tenant-a")` appends one event to it, either with
   `Events.Append` or with `Events.FetchForWriting<Tally>` followed by `AppendOne`.
3. The stream is read back from an untenanted query session.

The assertions describe what we expected from a store with no tenant dimension: the append
succeeds, `FetchForWriting` finds the aggregate, and the stream remains one stream whose
events are all under `*DEFAULT*`. **All six tests fail today**, and each store fails differently.

Each store has one test class (`marten_single_tenancy_appends`, `polecat_single_tenancy_appends`,
`fisher_single_tenancy_appends`) written with xunit v3 and Shouldly. Each runs on its own, so a
class can be moved into that store's test project with only its setup swapped for the local
harness.

## Results

| Store | `Append` from `tenant-a` | `FetchForWriting` from `tenant-a` |
|---|---|---|
| Marten 9.47.0 (PostgreSQL 16) | `MartenCommandException`: `P0001: The tenantid does not match the existing stream` (from `mt_quick_append_events`) | Same |
| Polecat 5.36.0 (SQL Server 2025) | `ExistingStreamIdCollisionException`: the stream lookup filters on the session tenant, misses the `*DEFAULT*` stream and tries to start it again | Same; `FetchForWriting` also returns a null aggregate |
| Fisher 1.19.0 (SQLite) | The append succeeds, but the new event is stored with `tenant_id = 'tenant-a'` while the stream row and the first event stay `*DEFAULT*` | Same |

So the three stores disagree. Fisher ignores the tenant for reads and concurrency checks under
`Single`, which is the behaviour we expected, but it still writes the session tenant onto the
new event row.

## How we got here

The tenanted sessions came from a mistake in how our application used Wolverine, not from
Marten. The repro uses `LightweightSession("tenant-a")` directly to keep Wolverine out of it,
but this is how it happened in the app.

Our modular monolith has two kinds of module: a global control plane (organisations, devices,
an identity registry), whose Marten stores are `Single`, and tenant-owned modules whose stores
are conjoined or routed to per-tenant databases.

**We treated tenancy as the exception.** Tenant-owned messages implement a marker interface,
`IDataTenancyOwnedMessage`, which carries the owning tenant's id. An `IEnvelopeRule` stamps
that id onto the envelope when the envelope has no tenant yet, and middleware rejects such a
message if it arrives without one. Our mental model was that tenant-owned messages opt in, and
everything else travels without a tenant.

**Wolverine treats tenancy as the default.** Once a handler is running for a tenant, everything
it sends or cascades inherits that tenant (`outbound.TenantId ??= TenantId` in `MessageBus`),
and HTTP endpoints with tenant detection open their session in the detected tenant. That is the
sensible default: work that starts in a tenant context stays in it unless you say otherwise.
Our rule only ever added a tenant and never removed one. So a command sent from tenant-owned
work to a global module (registering a person, or registering a device through an
organisation-scoped endpoint) still carried the sender's tenant. The global module's handler opened its `Single` store in that
tenant, and the append failed with the P0001 in the Marten results above.

For a while we worked around this with a session listener that rewrote pending streams to
`*DEFAULT*` before the save, which treated the symptom in the store.

**The fix we're making is to invert the pattern.** Global messages opt out, rather than
tenant-owned messages opting in:

- Commands handled by global modules implement an `IGlobalMessage` marker, and an envelope rule
  resets their tenant to `*DEFAULT*`. Wolverine applies metadata rules after tenant inheritance,
  so the rule wins over the inherited tenant on every send and cascade.
- HTTP endpoints backed only by a global store are marked `[NotTenanted]`.
- The listener becomes an assertion: a non-default tenant reaching a global store is a bug in
  the sender and should fail.

None of that changes the store behaviour shown above. A `Single` event store either enforces a
tenant it has no dimension for (Marten), can't find its own stream (Polecat), or records a
tenant it otherwise ignores (Fisher), and the three disagree. That inconsistency is what this
repro is for.

## Running

Requires the .NET 10 SDK and Docker. The Marten and Polecat tests use Testcontainers (Polecat
needs the `mcr.microsoft.com/mssql/server:2025-latest` image); Fisher uses a temporary SQLite
file.

```shell
dotnet test
```

The first run takes a minute or two while the images download.
