using JasperFx;
using JasperFx.Events;
using Fisher;
using Microsoft.Data.Sqlite;
using Shouldly;

namespace SingleTenancyRepro;

/// <summary>
///     With <c>Events.TenancyStyle = Single</c> Fisher ignores the tenant for stream reads and
///     concurrency checks, so a session opened for a tenant can append to a stream started
///     untenanted. But the insert still binds the session's tenant, so the new event row carries
///     <c>tenant_id = 'tenant-a'</c> while the stream row and the first event stay on
///     <c>*DEFAULT*</c>.
/// </summary>
public class fisher_single_tenancy_appends : IAsyncLifetime
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"fisher-single-tenancy-{Guid.NewGuid():N}.db");
    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        // Events.TenancyStyle is left at Single.
        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = $"Data Source={_file}";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
        });

        await _store.ApplyAllConfiguredChangesToDatabaseAsync(Token);
    }

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
        SqliteConnection.ClearAllPools();
        File.Delete(_file);
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
