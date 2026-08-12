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
            // Hive 4.x clients call get_databases(pattern) (catalog-prefixed) even for "show all",
            // so getAllDatabases()/SHOW DATABASES route here, not to get_all_databases.
            ["get_databases"] = (p, proto, header, ct) => p.HandleGetDatabasesAsync(proto, header, ct),
            ["get_database"] = (p, proto, header, ct) => p.HandleGetDatabaseAsync(proto, header, ct),
            ["create_database"] = (p, proto, header, ct) => p.HandleCreateDatabaseAsync(proto, header, ct),
            ["create_database_req"] = (p, proto, header, ct) => p.HandleCreateDatabaseReqAsync(proto, header, ct),
            ["drop_database"] = (p, proto, header, ct) => p.HandleDropDatabaseAsync(proto, header, ct),
            ["get_all_tables"] = (p, proto, header, ct) => p.HandleGetAllTablesAsync(proto, header, ct),
            ["get_tables"] = (p, proto, header, ct) => p.HandleGetTablesAsync(proto, header, ct),
            ["get_table"] = (p, proto, header, ct) => p.HandleGetTableAsync(proto, header, ct),
            // Hive 4.x request-wrapped variant — used by query compilation (e.g. SELECT ... looks up
            // the table via get_table_req, not get_table).
            ["get_table_req"] = (p, proto, header, ct) => p.HandleGetTableReqAsync(proto, header, ct),
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
            ["get_active_resource_plan"] = (p, proto, header, ct) => p.HandleGetActiveResourcePlanAsync(proto, header, ct),

            // ── Hive 4.x request-wrapped variants (clients call these, not the bare methods) ──
            ["get_database_req"] = (p, proto, header, ct) => p.HandleGetDatabaseReqAsync(proto, header, ct),
            ["create_table_req"] = (p, proto, header, ct) => p.HandleCreateTableReqAsync(proto, header, ct),
            ["alter_table_req"] = (p, proto, header, ct) => p.HandleAlterTableReqAsync(proto, header, ct),
            ["drop_table_with_environment_context"] = (p, proto, header, ct) => p.HandleDropTableWithCtxAsync(proto, header, ct),
            ["get_table_objects_by_name_req"] = (p, proto, header, ct) => p.HandleGetTableObjectsByNameReqAsync(proto, header, ct),

            // ── Planner / CBO stubs — we don't track constraints or column stats, so return empty
            //    (HS2 degrades gracefully: no keys, no stats-based costing). Keeps queries compiling.
            ["get_all_table_constraints"] = (p, proto, header, ct) => p.HandleGetAllTableConstraintsAsync(proto, header, ct),
            ["get_primary_keys"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_primary_keys", 1, ct),
            ["get_foreign_keys"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_foreign_keys", 1, ct),
            ["get_unique_constraints"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_unique_constraints", 1, ct),
            ["get_not_null_constraints"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_not_null_constraints", 1, ct),
            ["get_check_constraints"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_check_constraints", 1, ct),
            ["get_default_constraints"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_default_constraints", 1, ct),
            ["get_table_statistics_req"] = (p, proto, header, ct) => p.HandleEmptyListInStructAsync(proto, header, "get_table_statistics_req", 1, ct),
            ["get_partitions_by_expr"] = (p, proto, header, ct) => p.HandleGetPartitionsByExprAsync(proto, header, ct),
            ["get_aggr_stats_for"] = (p, proto, header, ct) => p.HandleGetAggrStatsForAsync(proto, header, ct),

            // ── Session/runtime calls ──
            ["get_config_value"] = (p, proto, header, ct) => p.HandleGetConfigValueAsync(proto, header, ct),
            ["getMetaConf"] = (p, proto, header, ct) => p.HandleGetMetaConfAsync(proto, header, ct),
            ["get_current_notificationEventId"] = (p, proto, header, ct) => p.HandleGetCurrentNotificationEventIdAsync(proto, header, ct),
            ["get_functions"] = (p, proto, header, ct) => p.HandleGetFunctionsAsync(proto, header, ct),
            ["flushCache"] = (p, proto, header, ct) => p.HandleFlushCacheAsync(proto, header, ct),
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

    private async Task HandleGetDatabasesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string pattern = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) pattern = await p.ReadStringAsync(ct);
            else await p.SkipAsync(ftype, ct);
        }, ct);
        var all = await handler.GetAllDatabasesAsync(ct);
        var filtered = FilterByHivePattern(all, StripCatalog(pattern));
        await WriteStringListReplyAsync(proto, "get_databases", header.SeqId, filtered, ct);
    }

    private async Task HandleGetDatabaseAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string name = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) name = await p.ReadStringAsync(ct);
            else await p.SkipAsync(ftype, ct);
        }, ct);

        var db = await handler.GetDatabaseAsync(StripCatalog(name), ct);
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) db = await ReadThriftDatabaseAsync(p, ct);
            else await p.SkipAsync(ftype, ct);
        }, ct);
        if (db is not null) await handler.CreateDatabaseAsync(db, ct);
        await WriteVoidReplyAsync(proto, "create_database", header.SeqId, ct);
    }

    private async Task HandleDropDatabaseAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string name = string.Empty; bool deleteData = false, cascade = false;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: deleteData = await p.ReadBoolAsync(ct); break;
                case 3: cascade = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        await handler.DropDatabaseAsync(name, deleteData, cascade, ct);
        await WriteVoidReplyAsync(proto, "drop_database", header.SeqId, ct);
    }

    // ── Table handlers ────────────────────────────────────────────────────────

    private async Task HandleGetAllTablesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) dbName = await p.ReadStringAsync(ct);
            else await p.SkipAsync(ftype, ct);
        }, ct);
        var tables = await handler.GetAllTablesAsync(StripCatalog(dbName), ct);
        await WriteStringListReplyAsync(proto, "get_all_tables", header.SeqId, tables, ct);
    }

    private async Task HandleGetTablesAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, pattern = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: pattern = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        var tables = await handler.GetTablesAsync(StripCatalog(dbName), pattern, ct);
        await WriteStringListReplyAsync(proto, "get_tables", header.SeqId, tables, ct);
    }

    private async Task HandleGetTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        var table = await handler.GetTableAsync(StripCatalog(dbName), tableName, ct);
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

    private async Task HandleGetTableReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // GetTableRequest has mixed field types (1:dbName, 2:tblName, 3:capabilities struct,
        // 4:catName, …), so we must skip unknown fields by their ACTUAL wire type, not a fixed one.
        string dbName = string.Empty, tblName = string.Empty;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var field = await proto.ReadFieldBeginAsync(ct);
            if (field.Type == TType.Stop) break;
            if (field.Id == 1 && field.Type == TType.String) dbName = await proto.ReadStringAsync(ct);
            else if (field.Id == 2 && field.Type == TType.String) tblName = await proto.ReadStringAsync(ct);
            else await proto.SkipAsync(field.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);

        var table = await handler.GetTableAsync(StripCatalog(dbName), tblName, ct);

        await proto.WriteMessageBeginAsync(new TMessage("get_table_req", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_table_req_result", ct);
        if (table is not null)
        {
            // success (0) = GetTableResult { 1: required Table table }
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await proto.WriteStructBeginAsync("GetTableResult", ct);
            await proto.WriteFieldBeginAsync(new TField("table", TType.Struct, 1), ct);
            await WriteThriftTableAsync(proto, table, ct);
            await proto.WriteFieldEndAsync(ct);
            await proto.WriteFieldStopAsync(ct);
            await proto.WriteStructEndAsync(ct);
            await proto.WriteFieldEndAsync(ct);
        }
        else
        {
            // o2 (2) = NoSuchObjectException { 1: message } — HS2 handles this (e.g. the virtual
            // _dummy_table for SELECT without FROM).
            await proto.WriteFieldBeginAsync(new TField("o2", TType.Struct, 2), ct);
            await proto.WriteStructBeginAsync("NoSuchObjectException", ct);
            await WriteStringField(proto, 1, $"{dbName}.{tblName} table not found", ct);
            await proto.WriteFieldStopAsync(ct);
            await proto.WriteStructEndAsync(ct);
            await proto.WriteFieldEndAsync(ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleCreateTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        ThriftTable? table = null;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) table = await ReadThriftTableAsync(p, ct);
            else await p.SkipAsync(ftype, ct);
        }, ct);
        if (table is not null) await handler.CreateTableAsync(table, ct);
        await WriteVoidReplyAsync(proto, "create_table", header.SeqId, ct);
    }

    private async Task HandleDropTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; bool deleteData = false;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: deleteData = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        await handler.DropTableAsync(dbName, tableName, deleteData, ct);
        await WriteVoidReplyAsync(proto, "drop_table", header.SeqId, ct);
    }

    private async Task HandleAlterTableAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty; ThriftTable? updated = null;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: updated = await ReadThriftTableAsync(p, ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        if (updated is not null) await handler.AlterTableAsync(dbName, tableName, updated, ct);
        await WriteVoidReplyAsync(proto, "alter_table", header.SeqId, ct);
    }

    // ── Schema handlers ───────────────────────────────────────────────────────

    private async Task HandleGetFieldsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        var fields = await handler.GetFieldsAsync(dbName, tableName, ct);
        await WriteFieldSchemaListReplyAsync(proto, "get_fields", header.SeqId, fields, ct);
    }

    private async Task HandleGetSchemaAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        var schema = await handler.GetSchemaAsync(dbName, tableName, ct);
        await WriteFieldSchemaListReplyAsync(proto, "get_schema", header.SeqId, schema, ct);
    }

    // ── Partition handlers ────────────────────────────────────────────────────

    private async Task HandleAddPartitionAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        ThriftPartition? partition = null;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            if (id == 1) partition = await ReadThriftPartitionAsync(p, ct);
            else await p.SkipAsync(ftype, ct);
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: values = await ReadStringListAsync(p, ct); break;
                default: await p.SkipAsync(ftype, ct); break;
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: maxParts = await p.ReadI32Async(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: maxParts = await p.ReadI32Async(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
            }
        }, ct);
        var names = await handler.GetPartitionNamesAsync(dbName, tableName, maxParts, ct);
        await WriteStringListReplyAsync(proto, "get_partition_names", header.SeqId, names, ct);
    }

    private async Task HandleDropPartitionAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        string dbName = string.Empty, tableName = string.Empty;
        var values = new List<string>(); bool deleteData = false;
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: dbName = await p.ReadStringAsync(ct); break;
                case 2: tableName = await p.ReadStringAsync(ct); break;
                case 3: values = await ReadStringListAsync(p, ct); break;
                case 4: deleteData = await p.ReadBoolAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            // field 1 = user (string), field 2 = groups (list<string>) — discard both
            if (id == 2)
                await ReadStringListAsync(p, ct);
            else
                await p.SkipAsync(ftype, ct);
        }, ct);
        // Return empty list<string>
        await proto.WriteMessageBeginAsync(new TMessage("set_ugi", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("set_ugi_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.List, 0), ct);
        await WriteStringListAsync(proto, [], ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetActiveResourcePlanAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // HiveServer2 calls this during startup (startOrReconnectTezSessions) to look for a
        // Workload Management resource plan. We don't implement WM, so return an empty
        // WMGetActiveResourcePlanResponse (resourcePlan unset = "no active plan"). HS2 then starts
        // with default Tez sessions instead of hanging on an unknown-method exception.
        await proto.SkipAsync(TType.Struct, ct); // WMGetActiveResourcePlanRequest — ignored
        await proto.WriteMessageBeginAsync(new TMessage("get_active_resource_plan", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_active_resource_plan_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("WMGetActiveResourcePlanResponse", ct);
        await proto.WriteFieldStopAsync(ct);   // resourcePlan optional + unset → empty struct
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    // ── Hive 4.x request-wrapped + planner/session handlers ───────────────────

    private async Task HandleGetDatabaseReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // GetDatabaseRequest { 1: string name, 2: string catalogName, ... }
        string name = string.Empty;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 1 && f.Type == TType.String) name = await proto.ReadStringAsync(ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);

        var db = await handler.GetDatabaseAsync(StripCatalog(name), ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_database_req", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_database_req_result", ct);
        if (db is not null)
        {
            await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
            await WriteThriftDatabaseAsync(proto, db, ct);
            await proto.WriteFieldEndAsync(ct);
        }
        else
        {
            await WriteNoSuchObjectFieldAsync(proto, 1, $"{name} database not found", ct);
        }
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleCreateDatabaseReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // CreateDatabaseRequest { 1: required Database database, ... }
        ThriftDatabase? db = null;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 1 && f.Type == TType.Struct) db = await ReadThriftDatabaseAsync(proto, ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
        if (db is not null) await handler.CreateDatabaseAsync(db, ct);
        await WriteVoidReplyAsync(proto, "create_database_req", header.SeqId, ct);
    }

    private async Task HandleCreateTableReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // CreateTableRequest { 1: required Table table, ... }
        ThriftTable? table = null;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 1 && f.Type == TType.Struct) table = await ReadThriftTableAsync(proto, ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
        if (table is not null) await handler.CreateTableAsync(table, ct);
        await WriteVoidReplyAsync(proto, "create_table_req", header.SeqId, ct);
    }

    private async Task HandleAlterTableReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // AlterTableRequest { 1: catName, 2: dbName, 3: tableName, 4: Table table, ... }
        string dbName = string.Empty, tableName = string.Empty; ThriftTable? updated = null;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 2 && f.Type == TType.String) dbName = await proto.ReadStringAsync(ct);
            else if (f.Id == 3 && f.Type == TType.String) tableName = await proto.ReadStringAsync(ct);
            else if (f.Id == 4 && f.Type == TType.Struct) updated = await ReadThriftTableAsync(proto, ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
        if (updated is not null) await handler.AlterTableAsync(StripCatalog(dbName), tableName, updated, ct);
        await WriteVoidReplyAsync(proto, "alter_table_req", header.SeqId, ct);
    }

    private async Task HandleDropTableWithCtxAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // drop_table_with_environment_context(1: dbname, 2: name, 3: deleteData, 4: EnvironmentContext)
        string dbName = string.Empty, tableName = string.Empty; bool deleteData = false;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 1 && f.Type == TType.String) dbName = await proto.ReadStringAsync(ct);
            else if (f.Id == 2 && f.Type == TType.String) tableName = await proto.ReadStringAsync(ct);
            else if (f.Id == 3 && f.Type == TType.Bool) deleteData = await proto.ReadBoolAsync(ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
        await handler.DropTableAsync(StripCatalog(dbName), tableName, deleteData, ct);
        await WriteVoidReplyAsync(proto, "drop_table_with_environment_context", header.SeqId, ct);
    }

    private async Task HandleGetTableObjectsByNameReqAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // GetTablesRequest { 1: dbName, 2: list<string> tblNames, ... } -> GetTablesResult { 1: list<Table> tables }
        string dbName = string.Empty; List<string> names = [];
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 1 && f.Type == TType.String) dbName = await proto.ReadStringAsync(ct);
            else if (f.Id == 2 && f.Type == TType.List) names = await ReadStringListAsync(proto, ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);

        var db = StripCatalog(dbName);
        var tables = new List<ThriftTable>();
        foreach (var n in names)
        {
            var t = await handler.GetTableAsync(db, n, ct);
            if (t is not null) tables.Add(t);
        }
        await proto.WriteMessageBeginAsync(new TMessage("get_table_objects_by_name_req", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_table_objects_by_name_req_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("GetTablesResult", ct);
        await proto.WriteFieldBeginAsync(new TField("tables", TType.List, 1), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, tables.Count), ct);
        foreach (var t in tables) await WriteThriftTableAsync(proto, t, ct);
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    /// <summary>Reply with success(0) = Response { &lt;listFieldId&gt;: [] } — an empty list of structs.
    /// Used for the constraint/stats fetches we don't track, which HS2 tolerates as "none".</summary>
    private async Task HandleEmptyListInStructAsync(ThriftBinaryProtocol proto, TMessage header, string method, short listFieldId, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync($"{method}_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("Response", ct);
        await WriteEmptyListFieldAsync(proto, listFieldId, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetAllTableConstraintsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // AllTableConstraintsResponse { 1: SQLAllTableConstraints { 1..6: constraint lists } } — all empty.
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_all_table_constraints", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_all_table_constraints_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("AllTableConstraintsResponse", ct);
        await proto.WriteFieldBeginAsync(new TField("allTableConstraints", TType.Struct, 1), ct);
        await proto.WriteStructBeginAsync("SQLAllTableConstraints", ct);
        for (short i = 1; i <= 6; i++) await WriteEmptyListFieldAsync(proto, i, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetPartitionsByExprAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // PartitionsByExprResult { 1: list<Partition> partitions, 2: bool hasUnknownPartitions }
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_partitions_by_expr", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_partitions_by_expr_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("PartitionsByExprResult", ct);
        await WriteEmptyListFieldAsync(proto, 1, ct);
        await WriteBoolField(proto, 2, false, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetAggrStatsForAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // AggrStats { 1: list<ColumnStatisticsObj> colStats, 2: i64 partsFound }
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_aggr_stats_for", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_aggr_stats_for_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("AggrStats", ct);
        await WriteEmptyListFieldAsync(proto, 1, ct);
        await WriteI64Field(proto, 2, 0, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetConfigValueAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // get_config_value(1: name, 2: defaultValue) -> string. We hold no server config → echo the default.
        string def = string.Empty;
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(ct);
            if (f.Type == TType.Stop) break;
            if (f.Id == 2 && f.Type == TType.String) def = await proto.ReadStringAsync(ct);
            else await proto.SkipAsync(f.Type, ct);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
        await WriteStringSuccessReplyAsync(proto, "get_config_value", header.SeqId, def, ct);
    }

    private async Task HandleGetMetaConfAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        await WriteStringSuccessReplyAsync(proto, "getMetaConf", header.SeqId, string.Empty, ct);
    }

    private async Task HandleGetCurrentNotificationEventIdAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        // CurrentNotificationEventId { 1: i64 eventId } — we don't keep a notification log → 0.
        await proto.SkipAsync(TType.Struct, ct);
        await proto.WriteMessageBeginAsync(new TMessage("get_current_notificationEventId", TMessageType.Reply, header.SeqId), ct);
        await proto.WriteStructBeginAsync("get_current_notificationEventId_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.Struct, 0), ct);
        await proto.WriteStructBeginAsync("CurrentNotificationEventId", ct);
        await WriteI64Field(proto, 1, 0, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private async Task HandleGetFunctionsAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        await WriteStringListReplyAsync(proto, "get_functions", header.SeqId, [], ct);
    }

    private async Task HandleFlushCacheAsync(ThriftBinaryProtocol proto, TMessage header, CancellationToken ct)
    {
        await proto.SkipAsync(TType.Struct, ct);
        await WriteVoidReplyAsync(proto, "flushCache", header.SeqId, ct);
    }

    // ── shared writers for the handlers above ─────────────────────────────────

    private static async Task WriteEmptyListFieldAsync(ThriftBinaryProtocol proto, short id, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.List, id), ct);
        await proto.WriteListBeginAsync(new TList(TType.Struct, 0), ct);
        await proto.WriteListEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteI64Field(ThriftBinaryProtocol proto, short id, long value, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField(string.Empty, TType.I64, id), ct);
        await proto.WriteI64Async(value, ct);
        await proto.WriteFieldEndAsync(ct);
    }

    private static async Task WriteStringSuccessReplyAsync(ThriftBinaryProtocol proto, string method, int seqId, string value, CancellationToken ct)
    {
        await proto.WriteMessageBeginAsync(new TMessage(method, TMessageType.Reply, seqId), ct);
        await proto.WriteStructBeginAsync($"{method}_result", ct);
        await proto.WriteFieldBeginAsync(new TField("success", TType.String, 0), ct);
        await proto.WriteStringAsync(value, ct);
        await proto.WriteFieldEndAsync(ct);
        await FinishStructAsync(proto, ct);
    }

    private static async Task WriteNoSuchObjectFieldAsync(ThriftBinaryProtocol proto, short fieldId, string message, CancellationToken ct)
    {
        await proto.WriteFieldBeginAsync(new TField("o2", TType.Struct, fieldId), ct);
        await proto.WriteStructBeginAsync("NoSuchObjectException", ct);
        await WriteStringField(proto, 1, message, ct);
        await proto.WriteFieldStopAsync(ct);
        await proto.WriteStructEndAsync(ct);
        await proto.WriteFieldEndAsync(ct);
    }

    // ── Hive 4.x catalog-qualified name handling ──────────────────────────────

    /// <summary>Hive 4.x clients prefix db names/patterns with the catalog:
    /// <c>@&lt;catalog&gt;#&lt;dbNameOrPattern&gt;</c>, and use <c>!</c> as the "no database" marker.
    /// We're single-catalog, so strip the prefix and map the empty marker to an empty pattern.</summary>
    private static string StripCatalog(string name)
    {
        if (name.Length > 0 && name[0] == '@')
        {
            var hash = name.IndexOf('#');
            if (hash >= 0) name = name[(hash + 1)..];
        }
        return name == "!" ? string.Empty : name;
    }

    /// <summary>Filter names by a Hive SHOW-pattern: <c>*</c> wildcard, <c>|</c> alternation,
    /// empty/<c>*</c> means "all". Case-insensitive, matching HMS semantics.</summary>
    private static IReadOnlyList<string> FilterByHivePattern(IReadOnlyList<string> names, string pattern)
    {
        if (string.IsNullOrEmpty(pattern) || pattern is "*" or ".*" or "%") return names;
        var regexes = pattern.Split('|', StringSplitOptions.RemoveEmptyEntries)
            .Select(a => "^" + System.Text.RegularExpressions.Regex.Escape(a).Replace("\\*", ".*") + "$")
            .ToList();
        return names.Where(n => regexes.Any(r =>
            System.Text.RegularExpressions.Regex.IsMatch(n, r, System.Text.RegularExpressions.RegexOptions.IgnoreCase)))
            .ToList();
    }

    // ── Struct r/w helpers ────────────────────────────────────────────────────

    // The field handler receives the wire TYPE too, so unknown/skipped fields are skipped by their
    // ACTUAL type. Skipping by a fixed type (e.g. String) corrupts the stream the moment a struct
    // carries a bool/i32/i64/struct/list field we don't read — which every Hive 4.x Table does.
    private static async Task ReadStructAsync(ThriftBinaryProtocol proto,
        Func<ThriftBinaryProtocol, short, TType, Task> fieldHandler, CancellationToken ct)
    {
        await proto.ReadStructBeginAsync(ct);
        while (true)
        {
            var field = await proto.ReadFieldBeginAsync(ct);
            if (field.Type == TType.Stop) break;
            await fieldHandler(proto, field.Id, field.Type);
            await proto.ReadFieldEndAsync(ct);
        }
        await proto.ReadStructEndAsync(ct);
    }

    // ── Thrift type r/w ───────────────────────────────────────────────────────

    private static async Task<ThriftDatabase> ReadThriftDatabaseAsync(ThriftBinaryProtocol proto, CancellationToken ct)
    {
        string name = "", locationUri = ""; string? description = null, ownerName = null;
        Dictionary<string, string> parameters = new();
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: description = await p.ReadStringAsync(ct); break;
                case 3: locationUri = await p.ReadStringAsync(ct); break;
                case 4: parameters = await ReadStringMapAsync(p, ct); break;
                case 5: ownerName = await p.ReadStringAsync(ct); break;
                default: await p.SkipAsync(ftype, ct); break;
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

        await ReadStructAsync(proto, async (p, id, ftype) =>
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
                default: await p.SkipAsync(ftype, ct); break;
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

        await ReadStructAsync(proto, async (p, id, ftype) =>
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
                default: await p.SkipAsync(ftype, ct); break;
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
        await ReadStructAsync(proto, async (p, id, ftype) =>
        {
            switch (id)
            {
                case 1: name = await p.ReadStringAsync(ct); break;
                case 2: lib = await p.ReadStringAsync(ct); break;
                case 3: parameters = await ReadStringMapAsync(p, ct); break;
                default: await p.SkipAsync(ftype, ct); break;
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

        await ReadStructAsync(proto, async (p, id, ftype) =>
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
                default: await p.SkipAsync(ftype, ct); break;
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
            await ReadStructAsync(proto, async (p, id, wt) =>
            {
                switch (id)
                {
                    case 1: fname = await p.ReadStringAsync(ct); break;
                    case 2: ftype = await p.ReadStringAsync(ct); break;
                    case 3: comment = await p.ReadStringAsync(ct); break;
                    default: await p.SkipAsync(wt, ct); break;
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
