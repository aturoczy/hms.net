using Hmsnet.Api.Thrift;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

/// <summary>
/// Tests for the hand-rolled Thrift binary protocol implementation.
/// Each test writes a value to a MemoryStream then reads it back,
/// verifying correct big-endian encoding and framing.
/// </summary>
[TestClass]
public class ThriftBinaryProtocolTests
{
    private static (ThriftBinaryProtocol writer, MemoryStream stream) MakeWriter()
    {
        var ms = new MemoryStream();
        return (new ThriftBinaryProtocol(ms), ms);
    }

    private static ThriftBinaryProtocol MakeReader(MemoryStream ms)
    {
        ms.Position = 0;
        return new ThriftBinaryProtocol(ms);
    }

    // ── Primitive round-trips ─────────────────────────────────────────────────

    [TestMethod]
    public async Task Bool_True_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteBoolAsync(true, CancellationToken.None);
        Assert.IsTrue(await MakeReader(ms).ReadBoolAsync(CancellationToken.None));
    }

    [TestMethod]
    public async Task Bool_False_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteBoolAsync(false, CancellationToken.None);
        Assert.IsFalse(await MakeReader(ms).ReadBoolAsync(CancellationToken.None));
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(-1)]
    [DataRow(int.MaxValue)]
    [DataRow(int.MinValue)]
    [DataRow(1_234_567)]
    public async Task I32_RoundTrip(int value)
    {
        var (w, ms) = MakeWriter();
        await w.WriteI32Async(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadI32Async(CancellationToken.None);
        Assert.AreEqual(value, result);
    }

    [TestMethod]
    [DataRow((short)0)]
    [DataRow((short)1)]
    [DataRow((short)-1)]
    [DataRow(short.MaxValue)]
    [DataRow(short.MinValue)]
    public async Task I16_RoundTrip(short value)
    {
        var (w, ms) = MakeWriter();
        await w.WriteI16Async(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadI16Async(CancellationToken.None);
        Assert.AreEqual(value, result);
    }

    [TestMethod]
    [DataRow(0L)]
    [DataRow(long.MaxValue)]
    [DataRow(long.MinValue)]
    [DataRow(1_700_000_000L)]
    public async Task I64_RoundTrip(long value)
    {
        var (w, ms) = MakeWriter();
        await w.WriteI64Async(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadI64Async(CancellationToken.None);
        Assert.AreEqual(value, result);
    }

    [TestMethod]
    [DataRow(0.0)]
    [DataRow(3.14159265358979)]
    [DataRow(-1.0)]
    [DataRow(double.MaxValue)]
    [DataRow(double.MinValue)]
    public async Task Double_RoundTrip(double value)
    {
        var (w, ms) = MakeWriter();
        await w.WriteDoubleAsync(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadDoubleAsync(CancellationToken.None);
        Assert.AreEqual(value, result, 1e-15);
    }

    [TestMethod]
    [DataRow("")]
    [DataRow("hello")]
    [DataRow("Apache Hive .NET Metastore")]
    [DataRow("unicode: \u4e2d\u6587\u6d4b\u8bd5")]
    public async Task String_RoundTrip(string value)
    {
        var (w, ms) = MakeWriter();
        await w.WriteStringAsync(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadStringAsync(CancellationToken.None);
        Assert.AreEqual(value, result);
    }

    [TestMethod]
    public async Task String_Long_RoundTrip()
    {
        var value = new string('x', 100_000);
        var (w, ms) = MakeWriter();
        await w.WriteStringAsync(value, CancellationToken.None);
        var result = await MakeReader(ms).ReadStringAsync(CancellationToken.None);
        Assert.AreEqual(value, result);
    }

    // ── Field framing ─────────────────────────────────────────────────────────

    [TestMethod]
    public async Task WriteField_Then_ReadField_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteFieldBeginAsync(new TField("name", TType.String, 42), CancellationToken.None);
        await w.WriteStringAsync("hive", CancellationToken.None);
        await w.WriteFieldEndAsync(CancellationToken.None);
        await w.WriteFieldStopAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var field = await r.ReadFieldBeginAsync(CancellationToken.None);

        Assert.AreEqual(TType.String, field.Type);
        Assert.AreEqual((short)42, field.Id);
        Assert.AreEqual("hive", await r.ReadStringAsync(CancellationToken.None));
        await r.ReadFieldEndAsync(CancellationToken.None);

        var stop = await r.ReadFieldBeginAsync(CancellationToken.None);
        Assert.AreEqual(TType.Stop, stop.Type);
    }

    // ── List framing ──────────────────────────────────────────────────────────

    [TestMethod]
    public async Task StringList_RoundTrip()
    {
        var items = new[] { "alpha", "bravo", "charlie" };
        var (w, ms) = MakeWriter();
        await w.WriteListBeginAsync(new TList(TType.String, items.Length), CancellationToken.None);
        foreach (var s in items) await w.WriteStringAsync(s, CancellationToken.None);
        await w.WriteListEndAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var list = await r.ReadListBeginAsync(CancellationToken.None);

        Assert.AreEqual(TType.String, list.ElementType);
        Assert.AreEqual(3, list.Count);

        var read = new List<string>();
        for (int i = 0; i < list.Count; i++)
            read.Add(await r.ReadStringAsync(CancellationToken.None));
        await r.ReadListEndAsync(CancellationToken.None);

        CollectionAssert.AreEqual(items, read);
    }

    [TestMethod]
    public async Task EmptyList_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteListBeginAsync(new TList(TType.String, 0), CancellationToken.None);
        await w.WriteListEndAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var list = await r.ReadListBeginAsync(CancellationToken.None);
        Assert.AreEqual(0, list.Count);
    }

    // ── Map framing ───────────────────────────────────────────────────────────

    [TestMethod]
    public async Task StringMap_RoundTrip()
    {
        var map = new Dictionary<string, string>
        {
            ["key1"] = "val1",
            ["key2"] = "val2",
            ["field.delim"] = ","
        };
        var (w, ms) = MakeWriter();
        await w.WriteMapBeginAsync(new TMap(TType.String, TType.String, map.Count), CancellationToken.None);
        foreach (var (k, v) in map)
        {
            await w.WriteStringAsync(k, CancellationToken.None);
            await w.WriteStringAsync(v, CancellationToken.None);
        }
        await w.WriteMapEndAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var mapHeader = await r.ReadMapBeginAsync(CancellationToken.None);
        Assert.AreEqual(3, mapHeader.Count);

        var result = new Dictionary<string, string>();
        for (int i = 0; i < mapHeader.Count; i++)
        {
            var k = await r.ReadStringAsync(CancellationToken.None);
            var v = await r.ReadStringAsync(CancellationToken.None);
            result[k] = v;
        }
        await r.ReadMapEndAsync(CancellationToken.None);

        Assert.AreEqual("val1", result["key1"]);
        Assert.AreEqual(",", result["field.delim"]);
    }

    // ── Message header ────────────────────────────────────────────────────────

    [TestMethod]
    public async Task Message_StrictFormat_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteMessageBeginAsync(new TMessage("get_table", TMessageType.Call, 7), CancellationToken.None);
        await w.WriteMessageEndAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var msg = await r.ReadMessageBeginAsync(CancellationToken.None);

        Assert.AreEqual("get_table", msg.Name);
        Assert.AreEqual(TMessageType.Call, msg.Type);
        Assert.AreEqual(7, msg.SeqId);
    }

    [TestMethod]
    public async Task Message_ReplyType_RoundTrip()
    {
        var (w, ms) = MakeWriter();
        await w.WriteMessageBeginAsync(new TMessage("create_table", TMessageType.Reply, 42), CancellationToken.None);
        await w.WriteMessageEndAsync(CancellationToken.None);

        var r = MakeReader(ms);
        var msg = await r.ReadMessageBeginAsync(CancellationToken.None);

        Assert.AreEqual("create_table", msg.Name);
        Assert.AreEqual(TMessageType.Reply, msg.Type);
        Assert.AreEqual(42, msg.SeqId);
    }

    // ── Skip ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task Skip_String_ConsumesCorrectBytes()
    {
        var (w, ms) = MakeWriter();
        await w.WriteStringAsync("skip_me", CancellationToken.None);
        await w.WriteI32Async(99, CancellationToken.None);   // sentinel value after skipped string

        var r = MakeReader(ms);
        await r.SkipAsync(TType.String, CancellationToken.None);
        var sentinel = await r.ReadI32Async(CancellationToken.None);

        Assert.AreEqual(99, sentinel);
    }

    [TestMethod]
    public async Task Skip_Struct_ConsumesAllNestedFields()
    {
        var (w, ms) = MakeWriter();
        // Write a nested struct
        await w.WriteStructBeginAsync("Inner", CancellationToken.None);
        await w.WriteFieldBeginAsync(new TField("f1", TType.String, 1), CancellationToken.None);
        await w.WriteStringAsync("nested_value", CancellationToken.None);
        await w.WriteFieldEndAsync(CancellationToken.None);
        await w.WriteFieldBeginAsync(new TField("f2", TType.I32, 2), CancellationToken.None);
        await w.WriteI32Async(123, CancellationToken.None);
        await w.WriteFieldEndAsync(CancellationToken.None);
        await w.WriteFieldStopAsync(CancellationToken.None);
        await w.WriteStructEndAsync(CancellationToken.None);
        await w.WriteI32Async(777, CancellationToken.None); // sentinel after struct

        var r = MakeReader(ms);
        await r.SkipAsync(TType.Struct, CancellationToken.None);
        var sentinel = await r.ReadI32Async(CancellationToken.None);

        Assert.AreEqual(777, sentinel);
    }

    [TestMethod]
    public async Task Skip_List_ConsumesAllElements()
    {
        var (w, ms) = MakeWriter();
        await w.WriteListBeginAsync(new TList(TType.I32, 5), CancellationToken.None);
        for (int i = 0; i < 5; i++) await w.WriteI32Async(i * 100, CancellationToken.None);
        await w.WriteListEndAsync(CancellationToken.None);
        await w.WriteI32Async(555, CancellationToken.None);

        var r = MakeReader(ms);
        await r.SkipAsync(TType.List, CancellationToken.None);
        Assert.AreEqual(555, await r.ReadI32Async(CancellationToken.None));
    }

    // ── Multiple sequential values ────────────────────────────────────────────

    [TestMethod]
    public async Task MultipleValues_WriteAndReadSequentially()
    {
        var (w, ms) = MakeWriter();
        await w.WriteI32Async(1, CancellationToken.None);
        await w.WriteStringAsync("two", CancellationToken.None);
        await w.WriteBoolAsync(true, CancellationToken.None);
        await w.WriteI64Async(3_000_000_000L, CancellationToken.None);

        var r = MakeReader(ms);
        Assert.AreEqual(1, await r.ReadI32Async(CancellationToken.None));
        Assert.AreEqual("two", await r.ReadStringAsync(CancellationToken.None));
        Assert.IsTrue(await r.ReadBoolAsync(CancellationToken.None));
        Assert.AreEqual(3_000_000_000L, await r.ReadI64Async(CancellationToken.None));
    }

    // ── Big-endian encoding ───────────────────────────────────────────────────

    [TestMethod]
    public async Task I32_IsBigEndian()
    {
        var (w, ms) = MakeWriter();
        await w.WriteI32Async(0x01020304, CancellationToken.None);

        var bytes = ms.ToArray();
        Assert.AreEqual(4, bytes.Length);
        Assert.AreEqual(0x01, bytes[0]);
        Assert.AreEqual(0x02, bytes[1]);
        Assert.AreEqual(0x03, bytes[2]);
        Assert.AreEqual(0x04, bytes[3]);
    }

    [TestMethod]
    public async Task I16_IsBigEndian()
    {
        var (w, ms) = MakeWriter();
        await w.WriteI16Async(0x0102, CancellationToken.None);

        var bytes = ms.ToArray();
        Assert.AreEqual(2, bytes.Length);
        Assert.AreEqual(0x01, bytes[0]);
        Assert.AreEqual(0x02, bytes[1]);
    }
}
