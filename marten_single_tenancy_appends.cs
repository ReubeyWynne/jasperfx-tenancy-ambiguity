using JasperFx;
using JasperFx.Events;
using Marten;
using Shouldly;
using Testcontainers.PostgreSql;

namespace SingleTenancyRepro;

/// <summary>
///     With <c>Events.TenancyStyle = Single</c> the event store has no tenant dimension, but a
///     session opened for a tenant still stamps that tenant on its <c>StreamAction</c>. Quick
///     append then compares it with the stream's stored <c>tenant_id</c> in
///     <c>mt_quick_append_events</c>, so appending to a stream started untenanted fails with
///     <c>P0001: The tenantid does not match the existing stream</c>.
/// </summary>
public class marten_single_tenancy_appends : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16").Build();
    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        await _postgres.StartAsync(Token);

        // Events.TenancyStyle is left at Single, and the append mode at its quick default.
        _store = DocumentStore.For(opts =>
        {
            opts.Connection(_postgres.GetConnectionString());
            opts.AutoCreateSchemaObjects = AutoCreate.All;
        });

        await _store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private async Task<Guid> aStreamStartedUntenanted()
    {
        await using var session = _store.LightweightSession();
        var streamId = session.Events.StartStream<Tally>(new Counted(1)).Id;
        await session.SaveChangesAsync(Token);

        return streamId;
    }

    [Fact]
    public async Task append_from_a_tenanted_session()
    {
        var streamId = await aStreamStartedUntenanted();

        await using (var session = _store.LightweightSession("tenant-a"))
        {
            session.Events.Append(streamId, new Counted(2));
            await session.SaveChangesAsync(Token);
        }

        await using var query = _store.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Token);
        events.Select(x => x.TenantId).ShouldBe([StorageConstants.DefaultTenantId, StorageConstants.DefaultTenantId]);
    }

    [Fact]
    public async Task fetch_for_writing_from_a_tenanted_session()
    {
        var streamId = await aStreamStartedUntenanted();

        Tally? fetched;
        await using (var session = _store.LightweightSession("tenant-a"))
        {
            var stream = await session.Events.FetchForWriting<Tally>(streamId, Token);
            fetched = stream.Aggregate;
            stream.AppendOne(new Counted(2));
            await session.SaveChangesAsync(Token);
        }

        fetched.ShouldNotBeNull().Total.ShouldBe(1);

        await using var query = _store.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Token);
        events.Select(x => x.TenantId).ShouldBe([StorageConstants.DefaultTenantId, StorageConstants.DefaultTenantId]);
    }
}
