using Hmsnet.Core.Exceptions;

namespace Hmsnet.Api.Thrift;

/// <summary>
/// Wire-level dispatcher for the ThriftHiveMetastore service.
/// Reads a Thrift binary-encoded message from the protocol, dispatches to the
/// appropriate method on <see cref="ThriftHmsHandler"/>, and writes the reply.
/// </summary>
public sealed class HiveMetastoreProcessor(ThriftHmsHandler handler)
{
    private static readonly Dictionary<string, Func<HiveMetastoreProcessor, ThriftBinaryProtocol, TMessage, CancellationToken, Task>> Dispatch =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["get_all_databases"] = (p, proto, header, ct) => p.HandleGetAllDatabasesAsync(proto, header, ct),
            ["get_database"] = (p, proto, header, ct) => p.HandleGetDatabaseAsync(proto, header, ct),
            ["create_database"] = (p, proto, header, ct) => p.HandleCreateDatabaseAsync(proto, header, ct),
            ["drop_database"] = (p, proto, header, ct) => p.HandleDropDatabaseAsync(proto, header, ct),
            ["get_all_tables"] = (p, proto, header, ct) => p.HandleGetAllTablesAsync(proto, header, ct),
            ["get_tables"] = (p, proto, header, ct) => p.HandleGetTablesAsync(proto, header, ct),
            ["get_table"] = (p, proto, header, ct) => p.HandleGetTableAsync(proto, header, ct),
            ["create_table"] = (p, proto, header, ct) => p.HandleCreateTableAsync(proto, header, ct),
            ["drop_table"] = (p, proto, header, ct) => p.HandleDropTableAsync(proto, header, ct),
            ["alter_table"] = (p, proto, header, ct) => p.HandleAlterTableAsync(proto, header, ct),
            ["get_fields"] = (p, proto, header, ct) => p.HandleGetFieldsAsync(proto, header, ct),
            ["get_schema"] = (p, proto, header, ct) => p.HandleGetSchemaAsync(proto, header, ct),
            ["add_partition"] = (p, proto, header, ct) => p.HandleAddPartitionAsync(proto, header, ct),
            ["get_partition"] = (p, proto, header, ct) => p.HandleGetPartitionAsync(proto, header, ct),
            ["get_partitions"] = (p, proto, header, ct) => p.HandleGetPartitionsAsync(proto, header, ct),
            ["get_partition_names"] = (p, proto, header, ct) => p.HandleGetPartitionNamesAsync(proto, header, ct),
            ["drop_partition"] = (p, proto, header, ct) => p.HandleDropPartitionAsync(proto, header, ct),
            // Stubs required by HiveServer2 startup — return empty/no-op responses
            ["get_all_functions"] = (p, proto, header, ct) => p.HandleGetAllFunctionsAsync(proto, header, ct),
            ["set_ugi"] = (p, proto, header, ct) => p.HandleSetUgiAsync(proto, header, ct),
        };

    public async Task ProcessAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        var msg = await proto.ReadMessageBeginAsync(ct);

        if (!Dispatch.TryGetValue(msg.Name, out var method))
        {
            await proto.SkipAsync(TType.Struct, ct);
            await proto.WriteMessageBeginAsync(new TMessage(msg.Name, TMessageType.Exception, msg.SeqId), ct);
            await WriteApplicationExceptionAsync(proto, 1, $"Unknown method: {msg.Name}", ct);
            await proto.WriteMessageEndAsync(ct);
            await proto.FlushAsync(ct);
            return;
        }

        try
        {
            await method(this, proto, msg, ct);
        }
        catch (MetastoreException ex)
        {
            await proto.WriteMessageBeginAsync(new TMessage(msg.Name, TMessageType.Exception, msg.SeqId), ct);
            await WriteApplicationExceptionAsync(proto, 6, ex.Message, ct);
            await proto.WriteMessageEndAsync(ct);
            await proto.FlushAsync(ct);
        }
    }

    // ── Database handlers ─────────────────────────────────────────────────────

    private async Task HandleGetAllDatabasesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        var names = await handler.GetAllDatabasesAsync(ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_all_databases", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_all_databases_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await WriteStringListAsync(proto, names, ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetDatabaseAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string name = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            if (id == 1) name = await p.ReadStringAsync(ct);
            else await p.SkipAsync(TType.String, ct);
        }, ct);

        var db = await handler.GetDatabaseAsync(name, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_database", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_database_result", ct);
        if (db is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await WriteThriftDatabaseAsync(proto, db, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleCreateDatabaseAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        ThriftDatabase? db = null;
        await ReadStructAsync(proto, async (p, id) =>
        {
            if (id == 1) db = await ReadThriftDatabaseAsync(p, ct);
            else await p.SkipAsync(TType.Struct, ct);
        }, ct);
        if (db is not null) await handler.CreateDatabaseAsync(db, ct);
        await WriteVoidReplyAsync(proto, "create_database", header.SeqId, ct);
    }

    private async Task HandleDropDatabaseAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string name = string.Empty; bool deleteData = false, cascade = false;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: deleteData = await p.ReadBoolAsync(ct); break;
                case 3: cascade = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        await handler.DropDatabaseAsync(name, deleteData, cascade, ct);
        await WriteVoidReplyAsync(proto, "drop_database", header.SeqId, ct);
    }

    // ── Table handlers ────────────────────────────────────────────────────────

    private async Task HandleGetAllTablesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            if (id == 1) dbName = await p.ReadStringAsync(ct);
            else await p.SkipAsync(TType.String, ct);
        }, ct);
        var tables = await handler.GetAllTablesAsync(dbName, ct);
        await WriteStringListReplyAsync(proto, "get_all_tables", header.SeqId, tables, ct);
    }

    private async Task HandleGetTablesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, pattern = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: pattern = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        var tables = await handler.GetTablesAsync(dbName, pattern, ct);
        await WriteStringListReplyAsync(proto, "get_tables", header.SeqId, tables, ct);
    }

    private async Task HandleGetTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        var table = await handler.GetTableAsync(dbName, tableName, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_table", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_table_result", ct);
        if (table is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await WriteThriftTableAsync(proto, table, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleCreateTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        ThriftTable? table = null;
        await ReadStructAsync(proto, async (p, id) =>
        {
            if (id == 1) table = await ReadThriftTableAsync(p, ct);
            else await p.SkipAsync(TType.Struct, ct);
        }, ct);
        if (table is not null) await handler.CreateTableAsync(table, ct);
        await WriteVoidReplyAsync(proto, "create_table", header.SeqId, ct);
    }

    private async Task HandleDropTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; bool deleteData = false;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: deleteData = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        await handler.DropTableAsync(dbName, tableName, deleteData, ct);
        await WriteVoidReplyAsync(proto, "drop_table", header.SeqId, ct);
    }

    private async Task HandleAlterTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; ThriftTable? updated = null;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: updated = await ReadThriftTableAsync(p, ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        if (updated is not null) await handler.AlterTableAsync(dbName, tableName, updated, ct);
        await WriteVoidReplyAsync(proto, "alter_table", header.SeqId, ct);
    }

    // ── Schema handlers ───────────────────────────────────────────────────────

    private async Task HandleGetFieldsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        var fields = await handler.GetFieldsAsync(dbName, tableName, ct);
        await WriteFieldSchemaListReplyAsync(proto, "get_fields", header.SeqId, fields, ct);
    }

    private async Task HandleGetSchemaAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        var schema = await handler.GetSchemaAsync(dbName, tableName, ct);
        await WriteFieldSchemaListReplyAsync(proto, "get_schema", header.SeqId, schema, ct);
    }

    // ── Partition handlers ────────────────────────────────────────────────────

    private async Task HandleAddPartitionAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        ThriftPartition? partition = null;
        await ReadStructAsync(proto, async (p, id) =>
        {
            if (id == 1) partition = await ReadThriftPartitionAsync(p, ct);
            else await p.SkipAsync(TType.Struct, ct);
        }, ct);
        ThriftPartition? result = partition is not null
            ? await handler.AddPartitionAsync(partition, ct) : null;
        await proto.WriteMessageBeginAsync(new TMessage("add_partition", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("add_partition_result", ct);
        if (result is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await WriteThriftPartitionAsync(proto, result, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetPartitionAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        var values = new List<string>();
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: values = await ReadStringListAsync(p, ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        var partition = await handler.GetPartitionAsync(dbName, tableName, values, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_partition", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_partition_result", ct);
        if (partition is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await WriteThriftPartitionAsync(proto, partition, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetPartitionsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; int maxParts = -1;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: maxParts = await p.ReadI32Async(ct); break;
                default: await p.SkipAsync(TType.I32, ct); break;
            }
        }, ct);
        var partitions = await handler.GetPartitionsAsync(dbName, tableName, maxParts, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_partitions", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_partitions_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, partitions.Count), ct);
        foreach (var p in partitions) await WriteThriftPartitionAsync(proto, p, ct);
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetPartitionNamesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; int maxParts = -1;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: maxParts = await p.ReadI32Async(ct); break;
                default: await p.SkipAsync(TType.I32, ct); break;
            }
        }, ct);
        var names = await handler.GetPartitionNamesAsync(dbName, tableName, maxParts, ct);
        await WriteStringListReplyAsync(proto, "get_partition_names", header.SeqId, names, ct);
    }

    private async Task HandleDropPartitionAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        var values = new List<string>(); bool deleteData = false;
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: values = await ReadStringListAsync(p, ct); break;
                case 4: deleteData = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(TType.Bool, ct); break;
            }
        }, ct);
        var result = await handler.DropPartitionAsync(dbName, tableName, values, deleteData, ct);
        await proto.WriteMessageBeginAsync(new TMessage("drop_partition", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("drop_partition_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Bool, 0), ct);
        await proto.WriteBoolAsync(result, ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    // ── Compatibility stubs ───────────────────────────────────────────────────

    /// <summary>
    /// Returns an empty list of registered functions. HiveServer2 calls this during
    /// startup to load UDFs from the metastore; an empty list is safe.
    /// </summary>
    private async Task HandleGetAllFunctionsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_all_functions", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_all_functions_result", ct);
        // field 0 = success: GetAllFunctionsResponse (struct with field 1 = list<Function>)
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("GetAllFunctionsResponse", ct);
        // field 1 = functions: list<Function> — write empty list
        await proto.WriteFieldBeginAsync(new TField("functions", TType.List, 1), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, 0), ct);
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    /// <summary>
    /// set_ugi is called by older Hive clients for simple authentication.
    /// We accept it as a no-op and return an empty group list.
    /// </summary>
    private async Task HandleSetUgiAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await ReadStructAsync(proto, async (p, id) =>
        {
            // field 1 = user (string), field 2 = groups (list<string>) — discard both
            if (id == 2)
                await ReadStringListAsync(p, ct);
            else
                await p.SkipAsync(TType.String, ct);
        }, ct);
        // Return empty list<string>
        await proto.WriteMessageBeginAsync(new TMessage("set_ugi", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("set_ugi_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await WriteStringListAsync(proto, [], ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    // ── Struct r/w helpers ────────────────────────────────────────────────────

    private static async Task ReadStructAsync(ThriftBinaryProtocol proto,
        Func<ThriftBinaryProtocol, short, Task> fieldHandler, CancellationToken ct)
    {
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var field = await proto.ReadFieldBeginAsync(ct);
            if (field.Type == TType.Stop) break;
            await fieldHandler(proto, field.Id);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
    }

    // ── Thrift type r/w ───────────────────────────────────────────────────────

    private static async Task<ThriftDatabase> ReadThriftDatabaseAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        string name = "", locationUri = ""; string? description = null, ownerName = null;
        Dictionary<string, string> parameters = new();
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: description = await p.ReadStringAsync(ct); break;
                case 3: locationUri = await p.ReadStringAsync(ct); break;
                case 4: parameters = await ReadStringMapAsync(p, ct); break;
                case 5: ownerName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        return new ThriftDatabase(name, description, locationUri, ownerName, parameters);
    }

    private static async Task WriteThriftDatabaseAsync(ThriftBinaryProtocol proto, ThriftDatabase db, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("Database", ct);
        await WriteStringField(proto, 1, db.Name, ct);
        if (db.Description is not null) await WriteStringField(proto, 2, db.Description, ct);
        await WriteStringField(proto, 3, db.LocationUri, ct);
        await WriteStringMapField(proto, 4, db.Parameters ?? new(), ct);
        if (db.OwnerName is not null) await WriteStringField(proto, 5, db.OwnerName, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }

    private static async Task<ThriftTable> ReadThriftTableAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        string tableName = "", dbName = "", tableType = "MANAGED_TABLE";
        string? owner = null, viewOrig = null, viewExp = null;
        int createTime = 0, lastAccess = 0, retention = 0;
        ThriftStorageDescriptor? sd = null;
        var partKeys = new List<ThriftFieldSchema>();
        Dictionary<string, string> parameters = new();

        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: tableName = await p.ReadStringAsync(ct); break;
                case 2: dbName = await p.ReadStringAsync(ct); break;
                case 3: owner = await p.ReadStringAsync(ct); break;
                case 4: createTime = await p.ReadI32Async(ct); break;
                case 5: lastAccess = await p.ReadI32Async(ct); break;
                case 6: retention = await p.ReadI32Async(ct); break;
                case 7: sd = await ReadThriftSdAsync(p, ct); break;
                case 8: parameters = await ReadStringMapAsync(p, ct); break;
                case 9: partKeys = await ReadFieldSchemaListAsync(p, ct); break;
                case 12: viewOrig = await p.ReadStringAsync(ct); break;
                case 13: viewExp = await p.ReadStringAsync(ct); break;
                case 15: tableType = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);

        return new ThriftTable(tableName, dbName, owner, tableType, createTime, lastAccess, retention,
            sd, null, partKeys, viewOrig, viewExp, parameters);
    }

    private static async Task WriteThriftTableAsync(ThriftBinaryProtocol proto, ThriftTable t, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("Table", ct);
        await WriteStringField(proto, 1, t.TableName, ct);
        await WriteStringField(proto, 2, t.DbName, ct);
        if (t.Owner is not null) await WriteStringField(proto, 3, t.Owner, ct);
        await WriteI32Field(proto, 4, t.CreateTime, ct);
        await WriteI32Field(proto, 5, t.LastAccessTime, ct);
        await WriteI32Field(proto, 6, t.Retention, ct);
        if (t.Sd is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("sd", TType.Struct, 7), ct);
            await WriteThriftSdAsync(proto, t.Sd, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await WriteStringMapField(proto, 8, t.Parameters ?? new(), ct);
        // field 9 = partitionKeys (list<FieldSchema>)
        var partKeys = t.PartitionKeys ?? [];
        await proto.WriteFieldBeginAsync(new TField("partitionKeys", TType.List, 9), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, partKeys.Count), ct);
        foreach (var pk in partKeys)
        {
            await proto.WriteStructBeginAsync("FieldSchema", ct);
            await WriteStringField(proto, 1, pk.Name, ct);
            await WriteStringField(proto, 2, pk.Type, ct);
            if (pk.Comment is not null) await WriteStringField(proto, 3, pk.Comment, ct);
            await proto.WriteFieldStopAsync(ct);
            await proto.WriteStructEndAsync(ct);
        }
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await WriteStringField(proto, 15, t.TableType, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }

    private static async Task<ThriftStorageDescriptor> ReadThriftSdAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        string location = "", inputFormat = "", outputFormat = "";
        bool compressed = false; int numBuckets = -1;
        ThriftSerDeInfo serDeInfo = new(null, string.Empty, null);
        Dictionary<string, string> parameters = new();
        List<ThriftFieldSchema> cols = new();

        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: cols = await ReadFieldSchemaListAsync(p, ct); break;
                case 4: location = await p.ReadStringAsync(ct); break;
                case 5: inputFormat = await p.ReadStringAsync(ct); break;
                case 6: outputFormat = await p.ReadStringAsync(ct); break;
                case 7: compressed = await p.ReadBoolAsync(ct); break;
                case 8: numBuckets = await p.ReadI32Async(ct); break;
                case 9: serDeInfo = await ReadThriftSerDeInfoAsync(p, ct); break;
                case 12: parameters = await ReadStringMapAsync(p, ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);

        return new ThriftStorageDescriptor(location, inputFormat, outputFormat, compressed, numBuckets,
            serDeInfo, null, null, parameters, cols.Count > 0 ? cols : null);
    }

    private static async Task WriteThriftSdAsync(ThriftBinaryProtocol proto, ThriftStorageDescriptor sd, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("StorageDescriptor", ct);
        // field 1 = cols (list<FieldSchema>) — data columns
        var cols = sd.Cols ?? [];
        await proto.WriteFieldBeginAsync(new TField("cols", TType.List, 1), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, cols.Count), ct);
        foreach (var f in cols)
        {
            await proto.WriteStructBeginAsync("FieldSchema", ct);
            await WriteStringField(proto, 1, f.Name, ct);
            await WriteStringField(proto, 2, f.Type, ct);
            if (f.Comment is not null) await WriteStringField(proto, 3, f.Comment, ct);
            await proto.WriteFieldStopAsync(ct);
            await proto.WriteStructEndAsync(ct);
        }
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await WriteStringField(proto, 4, sd.Location, ct);
        await WriteStringField(proto, 5, sd.InputFormat, ct);
        await WriteStringField(proto, 6, sd.OutputFormat, ct);
        await WriteBoolField(proto, 7, sd.Compressed, ct);
        await WriteI32Field(proto, 8, sd.NumBuckets, ct);
        await proto.WriteFieldBeginAsync(new TField("serDeInfo", TType.Struct, 9), ct);
        await WriteThriftSerDeInfoAsync(proto, sd.SerDeInfo, ct);
        await proto.WriteFieldEndAsync(ct);
        await WriteStringMapField(proto, 12, sd.Parameters ?? new(), ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }

    private static async Task<ThriftSerDeInfo> ReadThriftSerDeInfoAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        string? name = null; string lib = string.Empty;
        Dictionary<string, string> parameters = new();
        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: lib = await p.ReadStringAsync(ct); break;
                case 3: parameters = await ReadStringMapAsync(p, ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);
        return new ThriftSerDeInfo(name, lib, parameters);
    }

    private static async Task WriteThriftSerDeInfoAsync(ThriftBinaryProtocol proto, ThriftSerDeInfo info, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("SerDeInfo", ct);
        if (info.Name is not null) await WriteStringField(proto, 1, info.Name, ct);
        await WriteStringField(proto, 2, info.SerializationLib, ct);
        await WriteStringMapField(proto, 3, info.Parameters ?? new(), ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }

    private static async Task<ThriftPartition> ReadThriftPartitionAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        List<string> values = new(); string dbName = "", tableName = "";
        int createTime = 0, lastAccess = 0;
        ThriftStorageDescriptor? sd = null;
        Dictionary<string, string> parameters = new();

        await ReadStructAsync(proto, async (p, id) =>
        {
            switch (id)
            {
                case 1: values = await ReadStringListAsync(p, ct); break;
                case 2: dbName = await p.ReadStringAsync(ct); break;
                case 3: tableName = await p.ReadStringAsync(ct); break;
                case 4: createTime = await p.ReadI32Async(ct); break;
                case 5: lastAccess = await p.ReadI32Async(ct); break;
                case 6: sd = await ReadThriftSdAsync(p, ct); break;
                case 7: parameters = await ReadStringMapAsync(p, ct); break;
                default: await p.SkipAsync(TType.String, ct); break;
            }
        }, ct);

        return new ThriftPartition(values, dbName, tableName, createTime, lastAccess, sd, parameters);
    }

    private static async Task WriteThriftPartitionAsync(ThriftBinaryProtocol proto, ThriftPartition p, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("Partition", ct);
        // Write values list
        await proto.WriteFieldBeginAsync(new TField("values", TType.List, 1), ct);
        await WriteStringListAsync(proto, p.Values ?? new List<string>(), ct);
        await proto.WriteFieldEndAsync(ct);
        await WriteStringField(proto, 2, p.DbName, ct);
        await WriteStringField(proto, 3, p.TableName, ct);
        await WriteI32Field(proto, 4, p.CreateTime, ct);
        await WriteI32Field(proto, 5, p.LastAccessTime, ct);
        if (p.Sd is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("sd", TType.Struct, 6), ct);
            await WriteThriftSdAsync(proto, p.Sd, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await WriteStringMapField(proto, 7, p.Parameters ?? new(), ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }

    private static async Task<List<ThriftFieldSchema>> ReadFieldSchemaListAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        var list = await proto.ReadListBeginAsync(ct);
        var result = new List<ThriftFieldSchema>(list.Count);
        for (int i = 0; i < list.Count; i++)
        {
            string fname = "", ftype = ""; string? comment = null;
            await ReadStructAsync(proto, async (p, id) =>
            {
                switch (id)
                {
                    case 1: fname = await p.ReadStringAsync(ct); break;
                    case 2: ftype = await p.ReadStringAsync(ct); break;
                    case 3: comment = await p.ReadStringAsync(ct); break;
                    default: await p.SkipAsync(TType.String, ct); break;
                }
            }, ct);
            result.Add(new ThriftFieldSchema(fname, ftype, comment));
        }
        await proto.ReadListEndAsync(ct);
        return result;
    }

    private static async Task<List<string>> ReadStringListAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        var list = await proto.ReadListBeginAsync(ct);
        var result = new List<string>(list.Count);
        for (int i = 0; i < list.Count; i++) result.Add(await proto.ReadStringAsync(ct));
        await proto.ReadListEndAsync(ct);
        return result;
    }

    private static async Task<Dictionary<string, string>> ReadStringMapAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        var map = await proto.ReadMapBeginAsync(ct);
        var result = new Dictionary<string, string>(map.Count);
        for (int i = 0; i < map.Count; i++)
        {
            var k = await proto.ReadStringAsync(ct);
            var v = await proto.ReadStringAsync(ct);
            result[k] = v;
        }
        await proto.ReadMapEndAsync(ct);
        return result;
    }

    // ── Reply helpers ─────────────────────────────────────────────────────────

    private static async Task WriteVoidReplyAsync(ThriftBinaryProtocol proto, string method, int seqId, CancellationToken ct)
    {
        await proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Reply, seqId), ct);
        await proto.WriteStructBeginAsync($"{method}_result", ct);
        await FinishStructAsync(proto, ct);
    }

    private static async Task WriteStringListReplyAsync(ThriftBinaryProtocol proto, string method, int seqId,
        IReadOnlyList<string> items, CancellationToken ct)
    {
        await proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Reply, seqId), ct);
        await proto.WriteStructBeginAsync($"{method}_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await WriteStringListAsync(proto, items, ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private static async Task WriteFieldSchemaListReplyAsync(ThriftBinaryProtocol proto, string method, int seqId,
        IReadOnlyList<ThriftFieldSchema> fields, CancellationToken ct)
    {
        await proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Reply, seqId), ct);
        await proto.WriteStructBeginAsync($"{method}_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, fields.Count), ct);
        foreach (var f in fields)
        {
            await proto.WriteStructBeginAsync("FieldSchema", ct);
            await WriteStringField(proto, 1, f.Name, ct);
            await WriteStringField(proto, 2, f.Type, ct);
            if (f.Comment is not null) await WriteStringField(proto, 3, f.Comment, ct);
            await proto.WriteFieldStopAsync(ct);
            await proto.WriteStructEndAsync(ct);
        }
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private static async Task WriteStringListAsync(ThriftBinaryProtocol proto, IReadOnlyList<string> items, CancellationToken ct)
    {
        await proto.WriteListBeginAsync(new TList(TType.String, items.Count), ct);
        foreach (var s in items) await proto.WriteStringAsync(s, ct);
        await proto.WriteListEndAsync(ct);
    }

    private static async Task FinishStructAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteMessageEndAsync(ct);
        await proto.FlushAsync(ct);
    }

    // ── Field write primitives ────────────────────────────────────────────────

    private static async Task WriteStringField(ThriftBinaryProtocol proto, short id, string value, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.String, id), ct);
        await proto.WriteStringAsync(value, ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteI32Field(ThriftBinaryProtocol proto, short id, int value, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.I32, id), ct);
        await proto.WriteI32Async(value, ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteBoolField(ThriftBinaryProtocol proto, short id, bool value, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.Bool, id), ct);
        await proto.WriteBoolAsync(value, ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteStringMapField(ThriftBinaryProtocol proto, short id,
        Dictionary<string, string> map, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.Map, id), ct);
        await proto.WriteMapBeginAsync(new TMap(TType.String, TType.String, map.Count), ct);
        foreach (var (k, v) in map) { await proto.WriteStringAsync(k, ct); await proto.WriteStringAsync(v, ct); }
        await proto.WriteMapEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteApplicationExceptionAsync(ThriftBinaryProtocol proto, int type, string message, CancellationToken ct)
    {
        await proto.WriteStructBeginAsync("TApplicationException", ct);
        await WriteStringField(proto, 1, message, ct);
        await WriteI32Field(proto, 2, type, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
    }
}
