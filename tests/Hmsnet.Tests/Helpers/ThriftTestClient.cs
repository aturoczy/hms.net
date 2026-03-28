using System.Net;
using System.Net.Sockets;
using Hmsnet.Api.Thrift;

namespace Hmsnet.Tests.Helpers;

/// <summary>
/// Thrown when the server returns a TMessageType.Exception reply.
/// </summary>
public sealed class ThriftApplicationException : Exception
{
    public int TypeCode { get; }

    public ThriftApplicationException(string message, int typeCode)
        : base(message)
    {
        TypeCode = typeCode;
    }
}

/// <summary>
/// A real Thrift binary client that speaks the same wire format as the Java HMS client.
/// Maintains a single persistent TCP connection. Dispose to close.
/// </summary>
public sealed class ThriftTestClient : IAsyncDisposable
{
    private readonly TcpClient _tcp;
    private readonly ThriftBinaryProtocol _proto;
    private int _seqId;

    private ThriftTestClient(TcpClient tcp, ThriftBinaryProtocol proto)
    {
        _tcp = tcp;
        _proto = proto;
    }

    public static async Task<ThriftTestClient> ConnectAsync(int port, CancellationToken ct = default)
    {
        var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, ct);
        var proto = new ThriftBinaryProtocol(tcp.GetStream());
        return new ThriftTestClient(tcp, proto);
    }

    private int NextSeqId() => System.Threading.Interlocked.Increment(ref _seqId);

    // ── RPC helpers ───────────────────────────────────────────────────────────

    private async Task<TMessage> SendCallAsync(string method, CancellationToken ct)
    {
        int seq = NextSeqId();
        await _proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Call, seq), ct);
        return new TMessage(method, TMessageType.Call, seq);
    }

    private async Task FlushAndReadReplyAsync(CancellationToken ct)
    {
        await _proto.WriteMessageEndAsync(ct);
        await _proto.FlushAsync(ct);
        await ReadReplyHeaderAsync(ct);
    }

    private async Task ReadReplyHeaderAsync(CancellationToken ct)
    {
        var msg = await _proto.ReadMessageBeginAsync(ct);
        if (msg.Type == TMessageType.Exception)
        {
            var (exMsg, exType) = await ReadApplicationExceptionAsync(ct);
            throw new ThriftApplicationException(exMsg, exType);
        }
    }

    private async Task<(string message, int type)> ReadApplicationExceptionAsync(CancellationToken ct)
    {
        string msg = string.Empty; int typeCode = 0;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 1 when f.Type == TType.String: msg = await _proto.ReadStringAsync(ct); break;
                case 2 when f.Type == TType.I32: typeCode = await _proto.ReadI32Async(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return (msg, typeCode);
    }

    private async Task ReadVoidResultAsync(CancellationToken ct)
    {
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
    }

    // ── Database RPCs ─────────────────────────────────────────────────────────

    public async Task<List<string>> GetAllDatabasesAsync(CancellationToken ct = default)
    {
        await SendCallAsync("get_all_databases", ct);
        await _proto.WriteStructBeginAsync("get_all_databases_args", ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        var result = new List<string>();
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.List) result = await ReadStringListAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result;
    }

    public async Task CreateDatabaseAsync(string name, string? description = null, string? location = null,
        string? owner = null, Dictionary<string, string>? parameters = null, CancellationToken ct = default)
    {
        await SendCallAsync("create_database", ct);
        await _proto.WriteStructBeginAsync("create_database_args", ct);
        await _proto.WriteFieldBeginAsync(new TField("database", TType.Struct, 1), ct);
        await WriteThriftDatabaseAsync(name, description, location, owner, parameters, ct);
        await _proto.WriteFieldEndAsync(ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        await ReadVoidResultAsync(ct);
    }

    public async Task<ThriftDatabase> GetDatabaseAsync(string name, CancellationToken ct = default)
    {
        await SendCallAsync("get_database", ct);
        await _proto.WriteStructBeginAsync("get_database_args", ct);
        await WriteStringFieldAsync(1, name, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        ThriftDatabase? db = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.Struct) db = await ReadThriftDatabaseAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return db ?? throw new ThriftApplicationException("Database not found", 6);
    }

    public async Task DropDatabaseAsync(string name, bool deleteData = false, bool cascade = false, CancellationToken ct = default)
    {
        await SendCallAsync("drop_database", ct);
        await _proto.WriteStructBeginAsync("drop_database_args", ct);
        await WriteStringFieldAsync(1, name, ct);
        await WriteBoolFieldAsync(2, deleteData, ct);
        await WriteBoolFieldAsync(3, cascade, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        await ReadVoidResultAsync(ct);
    }

    // ── Table RPCs ────────────────────────────────────────────────────────────

    public async Task<List<string>> GetAllTablesAsync(string dbName, CancellationToken ct = default)
    {
        await SendCallAsync("get_all_tables", ct);
        await _proto.WriteStructBeginAsync("get_all_tables_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadStringListResultAsync(ct);
    }

    public async Task<List<string>> GetTablesAsync(string dbName, string pattern, CancellationToken ct = default)
    {
        await SendCallAsync("get_tables", ct);
        await _proto.WriteStructBeginAsync("get_tables_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, pattern, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadStringListResultAsync(ct);
    }

    public async Task CreateTableAsync(ThriftTable table, CancellationToken ct = default)
    {
        await SendCallAsync("create_table", ct);
        await _proto.WriteStructBeginAsync("create_table_args", ct);
        await _proto.WriteFieldBeginAsync(new TField("tbl", TType.Struct, 1), ct);
        await WriteThriftTableAsync(table, ct);
        await _proto.WriteFieldEndAsync(ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        await ReadVoidResultAsync(ct);
    }

    public async Task<ThriftTable> GetTableAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        await SendCallAsync("get_table", ct);
        await _proto.WriteStructBeginAsync("get_table_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        ThriftTable? table = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.Struct) table = await ReadThriftTableAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return table ?? throw new ThriftApplicationException("Table not found", 6);
    }

    public async Task DropTableAsync(string dbName, string tableName, bool deleteData = false, CancellationToken ct = default)
    {
        await SendCallAsync("drop_table", ct);
        await _proto.WriteStructBeginAsync("drop_table_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await WriteBoolFieldAsync(3, deleteData, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        await ReadVoidResultAsync(ct);
    }

    // ── Schema RPCs ───────────────────────────────────────────────────────────

    public async Task<List<ThriftFieldSchema>> GetFieldsAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        await SendCallAsync("get_fields", ct);
        await _proto.WriteStructBeginAsync("get_fields_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadFieldSchemaListResultAsync(ct);
    }

    public async Task<List<ThriftFieldSchema>> GetSchemaAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        await SendCallAsync("get_schema", ct);
        await _proto.WriteStructBeginAsync("get_schema_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadFieldSchemaListResultAsync(ct);
    }

    // ── Partition RPCs ────────────────────────────────────────────────────────

    public async Task<ThriftPartition> AddPartitionAsync(ThriftPartition partition, CancellationToken ct = default)
    {
        await SendCallAsync("add_partition", ct);
        await _proto.WriteStructBeginAsync("add_partition_args", ct);
        await _proto.WriteFieldBeginAsync(new TField("new_part", TType.Struct, 1), ct);
        await WriteThriftPartitionAsync(partition, ct);
        await _proto.WriteFieldEndAsync(ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        ThriftPartition? result = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.Struct) result = await ReadThriftPartitionAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result ?? throw new ThriftApplicationException("Partition not returned", 6);
    }

    public async Task<ThriftPartition> GetPartitionAsync(string dbName, string tableName, List<string> partVals, CancellationToken ct = default)
    {
        await SendCallAsync("get_partition", ct);
        await _proto.WriteStructBeginAsync("get_partition_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await _proto.WriteFieldBeginAsync(new TField("part_vals", TType.List, 3), ct);
        await WriteStringListAsync(partVals, ct);
        await _proto.WriteFieldEndAsync(ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        ThriftPartition? result = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.Struct) result = await ReadThriftPartitionAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result ?? throw new ThriftApplicationException("Partition not found", 6);
    }

    public async Task<List<ThriftPartition>> GetPartitionsAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default)
    {
        await SendCallAsync("get_partitions", ct);
        await _proto.WriteStructBeginAsync("get_partitions_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await WriteI32FieldAsync(3, maxParts, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        var result = new List<ThriftPartition>();
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.List)
            {
                var listInfo = await _proto.ReadListBeginAsync(ct);
                for (int i = 0; i < listInfo.Count; i++) result.Add(await ReadThriftPartitionAsync(ct));
                await _proto.ReadListEndAsync(ct);
            }
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result;
    }

    public async Task<List<string>> GetPartitionNamesAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default)
    {
        await SendCallAsync("get_partition_names", ct);
        await _proto.WriteStructBeginAsync("get_partition_names_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await WriteI32FieldAsync(3, maxParts, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadStringListResultAsync(ct);
    }

    public async Task<bool> DropPartitionAsync(string dbName, string tableName, List<string> partVals, bool deleteData = false, CancellationToken ct = default)
    {
        await SendCallAsync("drop_partition", ct);
        await _proto.WriteStructBeginAsync("drop_partition_args", ct);
        await WriteStringFieldAsync(1, dbName, ct);
        await WriteStringFieldAsync(2, tableName, ct);
        await _proto.WriteFieldBeginAsync(new TField("part_vals", TType.List, 3), ct);
        await WriteStringListAsync(partVals, ct);
        await _proto.WriteFieldEndAsync(ct);
        await WriteBoolFieldAsync(4, deleteData, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);

        bool result = false;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.Bool) result = await _proto.ReadBoolAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result;
    }

    // ── Compatibility stubs ───────────────────────────────────────────────────

    public async Task<bool> GetAllFunctionsAsync(CancellationToken ct = default)
    {
        await SendCallAsync("get_all_functions", ct);
        await _proto.WriteStructBeginAsync("get_all_functions_args", ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        await ReadVoidResultAsync(ct);
        return true;
    }

    public async Task<List<string>> SetUgiAsync(string user, List<string> groups, CancellationToken ct = default)
    {
        await SendCallAsync("set_ugi", ct);
        await _proto.WriteStructBeginAsync("set_ugi_args", ct);
        await WriteStringFieldAsync(1, user, ct);
        await _proto.WriteFieldBeginAsync(new TField("groups", TType.List, 2), ct);
        await WriteStringListAsync(groups, ct);
        await _proto.WriteFieldEndAsync(ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
        await FlushAndReadReplyAsync(ct);
        return await ReadStringListResultAsync(ct);
    }

    // ── Thrift struct writers ─────────────────────────────────────────────────

    private async Task WriteThriftDatabaseAsync(string name, string? description, string? location,
        string? owner, Dictionary<string, string>? parameters, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("Database", ct);
        await WriteStringFieldAsync(1, name, ct);
        if (description is not null) await WriteStringFieldAsync(2, description, ct);
        if (location is not null) await WriteStringFieldAsync(3, location, ct);
        if (parameters is not null) await WriteStringMapFieldAsync(4, parameters, ct);
        if (owner is not null) await WriteStringFieldAsync(5, owner, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    private async Task WriteThriftTableAsync(ThriftTable t, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("Table", ct);
        await WriteStringFieldAsync(1, t.TableName, ct);
        await WriteStringFieldAsync(2, t.DbName, ct);
        if (t.Owner is not null) await WriteStringFieldAsync(3, t.Owner, ct);
        await WriteI32FieldAsync(4, t.CreateTime, ct);
        await WriteI32FieldAsync(5, t.LastAccessTime, ct);
        await WriteI32FieldAsync(6, t.Retention, ct);
        if (t.Sd is not null)
        {
            await _proto.WriteFieldBeginAsync(new TField("sd", TType.Struct, 7), ct);
            // Pass data columns into SD.cols (field 1) per HMS wire protocol.
            var sdWithCols = t.Sd.Cols is not null ? t.Sd
                : new ThriftStorageDescriptor(t.Sd.Location, t.Sd.InputFormat, t.Sd.OutputFormat,
                    t.Sd.Compressed, t.Sd.NumBuckets, t.Sd.SerDeInfo,
                    t.Sd.BucketCols, t.Sd.SortCols, t.Sd.Parameters,
                    t.Columns);
            await WriteThriftSdAsync(sdWithCols, ct);
            await _proto.WriteFieldEndAsync(ct);
        }
        if (t.Parameters is not null) await WriteStringMapFieldAsync(8, t.Parameters, ct);
        var partKeys = t.PartitionKeys ?? [];
        await _proto.WriteFieldBeginAsync(new TField("partitionKeys", TType.List, 9), ct);
        await _proto.WriteListBeginAsync(new TList(TType.Struct, partKeys.Count), ct);
        foreach (var pk in partKeys) await WriteThriftFieldSchemaAsync(pk, ct);
        await _proto.WriteListEndAsync(ct);
        await _proto.WriteFieldEndAsync(ct);
        if (t.ViewOriginalText is not null) await WriteStringFieldAsync(12, t.ViewOriginalText, ct);
        if (t.ViewExpandedText is not null) await WriteStringFieldAsync(13, t.ViewExpandedText, ct);
        await WriteStringFieldAsync(15, t.TableType, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    private async Task WriteThriftSdAsync(ThriftStorageDescriptor sd, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("StorageDescriptor", ct);
        // Field 1: cols (data columns per HMS wire protocol)
        var cols = sd.Cols ?? [];
        await _proto.WriteFieldBeginAsync(new TField("cols", TType.List, 1), ct);
        await _proto.WriteListBeginAsync(new TList(TType.Struct, cols.Count), ct);
        foreach (var f in cols) await WriteThriftFieldSchemaAsync(f, ct);
        await _proto.WriteListEndAsync(ct);
        await _proto.WriteFieldEndAsync(ct);
        await WriteStringFieldAsync(4, sd.Location, ct);
        await WriteStringFieldAsync(5, sd.InputFormat, ct);
        await WriteStringFieldAsync(6, sd.OutputFormat, ct);
        await WriteBoolFieldAsync(7, sd.Compressed, ct);
        await WriteI32FieldAsync(8, sd.NumBuckets, ct);
        await _proto.WriteFieldBeginAsync(new TField("serDeInfo", TType.Struct, 9), ct);
        await WriteThriftSerDeInfoAsync(sd.SerDeInfo, ct);
        await _proto.WriteFieldEndAsync(ct);
        if (sd.Parameters is not null) await WriteStringMapFieldAsync(12, sd.Parameters, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    private async Task WriteThriftSerDeInfoAsync(ThriftSerDeInfo info, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("SerDeInfo", ct);
        if (info.Name is not null) await WriteStringFieldAsync(1, info.Name, ct);
        await WriteStringFieldAsync(2, info.SerializationLib, ct);
        if (info.Parameters is not null) await WriteStringMapFieldAsync(3, info.Parameters, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    private async Task WriteThriftFieldSchemaAsync(ThriftFieldSchema f, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("FieldSchema", ct);
        await WriteStringFieldAsync(1, f.Name, ct);
        await WriteStringFieldAsync(2, f.Type, ct);
        if (f.Comment is not null) await WriteStringFieldAsync(3, f.Comment, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    private async Task WriteThriftPartitionAsync(ThriftPartition p, CancellationToken ct)
    {
        await _proto.WriteStructBeginAsync("Partition", ct);
        await _proto.WriteFieldBeginAsync(new TField("values", TType.List, 1), ct);
        await WriteStringListAsync(p.Values ?? [], ct);
        await _proto.WriteFieldEndAsync(ct);
        await WriteStringFieldAsync(2, p.DbName, ct);
        await WriteStringFieldAsync(3, p.TableName, ct);
        await WriteI32FieldAsync(4, p.CreateTime, ct);
        await WriteI32FieldAsync(5, p.LastAccessTime, ct);
        if (p.Sd is not null)
        {
            await _proto.WriteFieldBeginAsync(new TField("sd", TType.Struct, 6), ct);
            await WriteThriftSdAsync(p.Sd, ct);
            await _proto.WriteFieldEndAsync(ct);
        }
        if (p.Parameters is not null) await WriteStringMapFieldAsync(7, p.Parameters, ct);
        await _proto.WriteFieldStopAsync(ct);
        await _proto.WriteStructEndAsync(ct);
    }

    // ── Thrift struct readers ─────────────────────────────────────────────────

    private async Task<ThriftDatabase> ReadThriftDatabaseAsync(CancellationToken ct)
    {
        string name = "", locationUri = ""; string? description = null, ownerName = null;
        Dictionary<string, string>? parameters = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 1 when f.Type == TType.String: name = await _proto.ReadStringAsync(ct); break;
                case 2 when f.Type == TType.String: description = await _proto.ReadStringAsync(ct); break;
                case 3 when f.Type == TType.String: locationUri = await _proto.ReadStringAsync(ct); break;
                case 4 when f.Type == TType.Map: parameters = await ReadStringMapAsync(ct); break;
                case 5 when f.Type == TType.String: ownerName = await _proto.ReadStringAsync(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        return new ThriftDatabase(name, description, locationUri, ownerName, parameters);
    }

    private async Task<ThriftTable> ReadThriftTableAsync(CancellationToken ct)
    {
        string tableName = "", dbName = "", tableType = "MANAGED_TABLE";
        string? owner = null, viewOrig = null, viewExp = null;
        int createTime = 0, lastAccess = 0, retention = 0;
        ThriftStorageDescriptor? sd = null;
        var partKeys = new List<ThriftFieldSchema>();
        Dictionary<string, string>? parameters = null;

        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 1 when f.Type == TType.String: tableName = await _proto.ReadStringAsync(ct); break;
                case 2 when f.Type == TType.String: dbName = await _proto.ReadStringAsync(ct); break;
                case 3 when f.Type == TType.String: owner = await _proto.ReadStringAsync(ct); break;
                case 4 when f.Type == TType.I32: createTime = await _proto.ReadI32Async(ct); break;
                case 5 when f.Type == TType.I32: lastAccess = await _proto.ReadI32Async(ct); break;
                case 6 when f.Type == TType.I32: retention = await _proto.ReadI32Async(ct); break;
                case 7 when f.Type == TType.Struct: sd = await ReadThriftSdAsync(ct); break;
                case 8 when f.Type == TType.Map: parameters = await ReadStringMapAsync(ct); break;
                case 9 when f.Type == TType.List: partKeys = await ReadFieldSchemaListAsync(ct); break;
                case 12 when f.Type == TType.String: viewOrig = await _proto.ReadStringAsync(ct); break;
                case 13 when f.Type == TType.String: viewExp = await _proto.ReadStringAsync(ct); break;
                case 15 when f.Type == TType.String: tableType = await _proto.ReadStringAsync(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        return new ThriftTable(tableName, dbName, owner, tableType, createTime, lastAccess, retention,
            sd, null, partKeys, viewOrig, viewExp, parameters);
    }

    private async Task<ThriftStorageDescriptor> ReadThriftSdAsync(CancellationToken ct)
    {
        string location = "", inputFormat = "", outputFormat = "";
        bool compressed = false; int numBuckets = -1;
        ThriftSerDeInfo serDeInfo = new(null, string.Empty, null);
        Dictionary<string, string>? parameters = null;

        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 4 when f.Type == TType.String: location = await _proto.ReadStringAsync(ct); break;
                case 5 when f.Type == TType.String: inputFormat = await _proto.ReadStringAsync(ct); break;
                case 6 when f.Type == TType.String: outputFormat = await _proto.ReadStringAsync(ct); break;
                case 7 when f.Type == TType.Bool: compressed = await _proto.ReadBoolAsync(ct); break;
                case 8 when f.Type == TType.I32: numBuckets = await _proto.ReadI32Async(ct); break;
                case 9 when f.Type == TType.Struct: serDeInfo = await ReadThriftSerDeInfoAsync(ct); break;
                case 12 when f.Type == TType.Map: parameters = await ReadStringMapAsync(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        return new ThriftStorageDescriptor(location, inputFormat, outputFormat, compressed, numBuckets,
            serDeInfo, null, null, parameters);
    }

    private async Task<ThriftSerDeInfo> ReadThriftSerDeInfoAsync(CancellationToken ct)
    {
        string? name = null; string lib = string.Empty; Dictionary<string, string>? parameters = null;
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 1 when f.Type == TType.String: name = await _proto.ReadStringAsync(ct); break;
                case 2 when f.Type == TType.String: lib = await _proto.ReadStringAsync(ct); break;
                case 3 when f.Type == TType.Map: parameters = await ReadStringMapAsync(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        return new ThriftSerDeInfo(name, lib, parameters);
    }

    private async Task<ThriftPartition> ReadThriftPartitionAsync(CancellationToken ct)
    {
        List<string> values = []; string dbName = "", tableName = "";
        int createTime = 0, lastAccess = 0;
        ThriftStorageDescriptor? sd = null;
        Dictionary<string, string>? parameters = null;

        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            switch (f.Id)
            {
                case 1 when f.Type == TType.List: values = await ReadStringListAsync(ct); break;
                case 2 when f.Type == TType.String: dbName = await _proto.ReadStringAsync(ct); break;
                case 3 when f.Type == TType.String: tableName = await _proto.ReadStringAsync(ct); break;
                case 4 when f.Type == TType.I32: createTime = await _proto.ReadI32Async(ct); break;
                case 5 when f.Type == TType.I32: lastAccess = await _proto.ReadI32Async(ct); break;
                case 6 when f.Type == TType.Struct: sd = await ReadThriftSdAsync(ct); break;
                case 7 when f.Type == TType.Map: parameters = await ReadStringMapAsync(ct); break;
                default: await _proto.SkipAsync(f.Type, ct); break;
            }
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        return new ThriftPartition(values, dbName, tableName, createTime, lastAccess, sd, parameters);
    }

    private async Task<List<ThriftFieldSchema>> ReadFieldSchemaListAsync(CancellationToken ct)
    {
        var listInfo = await _proto.ReadListBeginAsync(ct);
        var result = new List<ThriftFieldSchema>(listInfo.Count);
        for (int i = 0; i < listInfo.Count; i++)
        {
            string fname = "", ftype = ""; string? comment = null;
            await _proto.ReadStructBeginAsync(ct);
            while (true)
            {
                var f = await _proto.ReadFieldBeginAsync(ct);
                if (f.Type == TType.Stop) break;
                switch (f.Id)
                {
                    case 1 when f.Type == TType.String: fname = await _proto.ReadStringAsync(ct); break;
                    case 2 when f.Type == TType.String: ftype = await _proto.ReadStringAsync(ct); break;
                    case 3 when f.Type == TType.String: comment = await _proto.ReadStringAsync(ct); break;
                    default: await _proto.SkipAsync(f.Type, ct); break;
                }
                await _proto.ReadFieldEndAsync(ct);
            }
            await _proto.ReadStructEndAsync(ct);
            result.Add(new ThriftFieldSchema(fname, ftype, comment));
        }
        await _proto.ReadListEndAsync(ct);
        return result;
    }

    private async Task<List<string>> ReadStringListResultAsync(CancellationToken ct)
    {
        var result = new List<string>();
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.List) result = await ReadStringListAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result;
    }

    private async Task<List<ThriftFieldSchema>> ReadFieldSchemaListResultAsync(CancellationToken ct)
    {
        var result = new List<ThriftFieldSchema>();
        await _proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await _proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 0 && f.Type == TType.List) result = await ReadFieldSchemaListAsync(ct);
            else await _proto.SkipAsync(f.Type, ct);
            await _proto.ReadFieldEndAsync(ct);
        }
        await _proto.ReadStructEndAsync(ct);
        await _proto.ReadMessageEndAsync(ct);
        return result;
    }

    private async Task<List<string>> ReadStringListAsync(CancellationToken ct)
    {
        var listInfo = await _proto.ReadListBeginAsync(ct);
        var result = new List<string>(listInfo.Count);
        for (int i = 0; i < listInfo.Count; i++) result.Add(await _proto.ReadStringAsync(ct));
        await _proto.ReadListEndAsync(ct);
        return result;
    }

    private async Task<Dictionary<string, string>> ReadStringMapAsync(CancellationToken ct)
    {
        var mapInfo = await _proto.ReadMapBeginAsync(ct);
        var result = new Dictionary<string, string>(mapInfo.Count);
        for (int i = 0; i < mapInfo.Count; i++)
            result[await _proto.ReadStringAsync(ct)] = await _proto.ReadStringAsync(ct);
        await _proto.ReadMapEndAsync(ct);
        return result;
    }

    // ── Wire write helpers ────────────────────────────────────────────────────

    private async Task WriteStringFieldAsync(short id, string value, CancellationToken ct)
    {
        await _proto.WriteFieldBeginAsync(new TField(string.Empty, TType.String, id), ct);
        await _proto.WriteStringAsync(value, ct);
        await _proto.WriteFieldEndAsync(ct);
    }

    private async Task WriteI32FieldAsync(short id, int value, CancellationToken ct)
    {
        await _proto.WriteFieldBeginAsync(new TField(string.Empty, TType.I32, id), ct);
        await _proto.WriteI32Async(value, ct);
        await _proto.WriteFieldEndAsync(ct);
    }

    private async Task WriteBoolFieldAsync(short id, bool value, CancellationToken ct)
    {
        await _proto.WriteFieldBeginAsync(new TField(string.Empty, TType.Bool, id), ct);
        await _proto.WriteBoolAsync(value, ct);
        await _proto.WriteFieldEndAsync(ct);
    }

    private async Task WriteStringListAsync(IReadOnlyList<string> items, CancellationToken ct)
    {
        await _proto.WriteListBeginAsync(new TList(TType.String, items.Count), ct);
        foreach (var s in items) await _proto.WriteStringAsync(s, ct);
        await _proto.WriteListEndAsync(ct);
    }

    private async Task WriteStringMapFieldAsync(short id, Dictionary<string, string> map, CancellationToken ct)
    {
        await _proto.WriteFieldBeginAsync(new TField(string.Empty, TType.Map, id), ct);
        await _proto.WriteMapBeginAsync(new TMap(TType.String, TType.String, map.Count), ct);
        foreach (var (k, v) in map) { await _proto.WriteStringAsync(k, ct); await _proto.WriteStringAsync(v, ct); }
        await _proto.WriteMapEndAsync(ct);
        await _proto.WriteFieldEndAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        try { _tcp.Close(); } catch { }
        await Task.CompletedTask;
    }
}
