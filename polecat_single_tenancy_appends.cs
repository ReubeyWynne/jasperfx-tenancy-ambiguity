using JasperFx;
using JasperFx.Events;
using Polecat;
using Shouldly;
using Testcontainers.MsSql;

namespace SingleTenancyRepro;

/// <summary>
///     With <c>Events.TenancyStyle = Single</c> the event store has no tenant dimension, but
///     <c>FetchForWriting</c> and the append path read the stream with
///     <c>WHERE id = @id AND tenant_id = @tenant_id</c> using the session's tenant. A session opened
///     for a tenant can't see a stream started untenanted, treats it as new, and the insert into
///     <c>pc_streams</c> (keyed on <c>id</c> alone) throws <c>ExistingStreamIdCollisionException</c>.
/// </summary>
public class polecat_single_tenancy_appends : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest").Build();

    private DocumentStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        await _sqlServer.StartAsync(Token);

        // Events.TenancyStyle is left at Single.
        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = _sqlServer.GetConnectionString();
            opts.AutoCreateSchemaObjects = AutoCreate.All;
        });

        await _store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);
    }

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
        await _sqlServer.DisposeAsync();
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
