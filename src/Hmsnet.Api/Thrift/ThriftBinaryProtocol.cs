using System.Buffers.Binary;
using System.Text;

namespace Hmsnet.Api.Thrift;

/// <summary>
/// Thrift type codes (matches the Thrift IDL spec).
/// </summary>
public enum TType : byte
{
    Stop = 0, Void = 1, Bool = 2, Byte = 3, Double = 4,
    I16 = 6, I32 = 8, I64 = 10, String = 11,
    Struct = 12, Map = 13, Set = 14, List = 15
}

/// <summary>
/// Thrift message types.
/// </summary>
public enum TMessageType : byte { Call = 1, Reply = 2, Exception = 3, OneWay = 4 }

public readonly record struct TMessage(string Name, TMessageType Type, int SeqId);
public readonly record struct TField(string Name, TType Type, short Id);
public readonly record struct TList(TType ElementType, int Count);
public readonly record struct TMap(TType KeyType, TType ValueType, int Count);

/// <summary>
/// Pure .NET implementation of the Thrift Binary protocol over a <see cref="Stream"/>.
/// Supports both strict (version-prefixed) and non-strict message headers.
/// </summary>
public sealed class ThriftBinaryProtocol(Stream stream)
{
    private const int StrictVersionMask = unchecked((int)0xFFFF0000);
    private const int StrictVersion1 = unchecked((int)0x80010000);

    // ── Message ───────────────────────────────────────────────────────────────

    public async Task<TMessage> ReadMessageBeginAsync(CancellationToken ct)
    {
        int first = await ReadI32Async(ct);
        if ((first & StrictVersionMask) == StrictVersion1)
        {
            // Strict format: version | type, then name, then seqid
            var type = (TMessageType)(first & 0x000000FF);
            var name = await ReadStringAsync(ct);
            var seqId = await ReadI32Async(ct);
            return new TMessage(name, type, seqId);
        }
        else
        {
            // Non-strict: first int is name length
            var name = await ReadStringBytesAsync(first, ct);
            var typeByte = await ReadByteAsync(ct);
            var seqId = await ReadI32Async(ct);
            return new TMessage(name, (TMessageType)typeByte, seqId);
        }
    }

    public async Task WriteMessageBeginAsync(TMessage msg, CancellationToken ct)
    {
        await WriteI32Async(StrictVersion1 | (int)msg.Type, ct);
        await WriteStringAsync(msg.Name, ct);
        await WriteI32Async(msg.SeqId, ct);
    }

    public Task WriteMessageEndAsync(CancellationToken ct) => Task.CompletedTask;
    public Task ReadMessageEndAsync(CancellationToken ct) => Task.CompletedTask;

    // ── Struct ────────────────────────────────────────────────────────────────

    public Task WriteStructBeginAsync(string name, CancellationToken ct) => Task.CompletedTask;
    public Task WriteStructEndAsync(CancellationToken ct) => Task.CompletedTask;
    public Task ReadStructBeginAsync(CancellationToken ct) => Task.CompletedTask;
    public Task ReadStructEndAsync(CancellationToken ct) => Task.CompletedTask;

    // ── Field ─────────────────────────────────────────────────────────────────

    public async Task WriteFieldBeginAsync(TField field, CancellationToken ct)
    {
        await WriteByteAsync((byte)field.Type, ct);
        await WriteI16Async(field.Id, ct);
    }

    public Task WriteFieldEndAsync(CancellationToken ct) => Task.CompletedTask;

    public Task WriteFieldStopAsync(CancellationToken ct) =>
        WriteByteAsync((byte)TType.Stop, ct);

    public async Task<TField> ReadFieldBeginAsync(CancellationToken ct)
    {
        var type = (TType)await ReadByteAsync(ct);
        if (type == TType.Stop) return new TField(string.Empty, TType.Stop, 0);
        var id = await ReadI16Async(ct);
        return new TField(string.Empty, type, id);
    }

    public Task ReadFieldEndAsync(CancellationToken ct) => Task.CompletedTask;

    // ── List ──────────────────────────────────────────────────────────────────

    public async Task WriteListBeginAsync(TList list, CancellationToken ct)
    {
        await WriteByteAsync((byte)list.ElementType, ct);
        await WriteI32Async(list.Count, ct);
    }

    public Task WriteListEndAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task<TList> ReadListBeginAsync(CancellationToken ct)
    {
        var elemType = (TType)await ReadByteAsync(ct);
        var count = await ReadI32Async(ct);
        return new TList(elemType, count);
    }

    public Task ReadListEndAsync(CancellationToken ct) => Task.CompletedTask;

    // ── Map ───────────────────────────────────────────────────────────────────

    public async Task WriteMapBeginAsync(TMap map, CancellationToken ct)
    {
        await WriteByteAsync((byte)map.KeyType, ct);
        await WriteByteAsync((byte)map.ValueType, ct);
        await WriteI32Async(map.Count, ct);
    }

    public Task WriteMapEndAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task<TMap> ReadMapBeginAsync(CancellationToken ct)
    {
        var keyType = (TType)await ReadByteAsync(ct);
        var valType = (TType)await ReadByteAsync(ct);
        var count = await ReadI32Async(ct);
        return new TMap(keyType, valType, count);
    }

    public Task ReadMapEndAsync(CancellationToken ct) => Task.CompletedTask;

    // ── Primitives ────────────────────────────────────────────────────────────

    public async Task<bool> ReadBoolAsync(CancellationToken ct) =>
        await ReadByteAsync(ct) != 0;

    public Task WriteBoolAsync(bool value, CancellationToken ct) =>
        WriteByteAsync(value ? (byte)1 : (byte)0, ct);

    public async Task<int> ReadI32Async(CancellationToken ct)
    {
        var buf = new byte[4];
        await ReadFullAsync(buf, ct);
        return BinaryPrimitives.ReadInt32BigEndian(buf);
    }

    public async Task WriteI32Async(int value, CancellationToken ct)
    {
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buf, value);
        await stream.WriteAsync(buf, ct);
    }

    public async Task<short> ReadI16Async(CancellationToken ct)
    {
        var buf = new byte[2];
        await ReadFullAsync(buf, ct);
        return BinaryPrimitives.ReadInt16BigEndian(buf);
    }

    public async Task WriteI16Async(short value, CancellationToken ct)
    {
        var buf = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buf, value);
        await stream.WriteAsync(buf, ct);
    }

    public async Task<long> ReadI64Async(CancellationToken ct)
    {
        var buf = new byte[8];
        await ReadFullAsync(buf, ct);
        return BinaryPrimitives.ReadInt64BigEndian(buf);
    }

    public async Task WriteI64Async(long value, CancellationToken ct)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buf, value);
        await stream.WriteAsync(buf, ct);
    }

    public async Task<double> ReadDoubleAsync(CancellationToken ct)
    {
        var buf = new byte[8];
        await ReadFullAsync(buf, ct);
        var bits = BinaryPrimitives.ReadInt64BigEndian(buf);
        return BitConverter.Int64BitsToDouble(bits);
    }

    public async Task WriteDoubleAsync(double value, CancellationToken ct)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buf, BitConverter.DoubleToInt64Bits(value));
        await stream.WriteAsync(buf, ct);
    }

    public async Task<string> ReadStringAsync(CancellationToken ct)
    {
        var len = await ReadI32Async(ct);
        return await ReadStringBytesAsync(len, ct);
    }

    public async Task WriteStringAsync(string value, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        await WriteI32Async(bytes.Length, ct);
        await stream.WriteAsync(bytes, ct);
    }

    public async Task<byte> ReadByteAsync(CancellationToken ct)
    {
        var buf = new byte[1];
        await ReadFullAsync(buf, ct);
        return buf[0];
    }

    public async Task WriteByteAsync(byte value, CancellationToken ct) =>
        await stream.WriteAsync(new[] { value }, ct);

    public Task FlushAsync(CancellationToken ct) => stream.FlushAsync(ct);

    /// <summary>
    /// Skips over a value of the given type without reading it into memory.
    /// </summary>
    public async Task SkipAsync(TType type, CancellationToken ct)
    {
        switch (type)
        {
            case TType.Bool:
            case TType.Byte:   await ReadByteAsync(ct); break;
            case TType.I16:    await ReadI16Async(ct); break;
            case TType.I32:    await ReadI32Async(ct); break;
            case TType.I64:
            case TType.Double: await ReadI64Async(ct); break;
            case TType.String:
                var len = await ReadI32Async(ct);
                var buf = new byte[len];
                await ReadFullAsync(buf, ct);
                break;
            case TType.Struct:
                await ReadStructBeginAsync(ct);
                while (true)
                {
                    var f = await ReadFieldBeginAsync(ct);
                    if (f.Type == TType.Stop) break;
                    await SkipAsync(f.Type, ct);
                    await ReadFieldEndAsync(ct);
                }
                await ReadStructEndAsync(ct);
                break;
            case TType.List:
            case TType.Set:
                var list = await ReadListBeginAsync(ct);
                for (int i = 0; i < list.Count; i++) await SkipAsync(list.ElementType, ct);
                await ReadListEndAsync(ct);
                break;
            case TType.Map:
                var map = await ReadMapBeginAsync(ct);
                for (int i = 0; i < map.Count; i++)
                {
                    await SkipAsync(map.KeyType, ct);
                    await SkipAsync(map.ValueType, ct);
                }
                await ReadMapEndAsync(ct);
                break;
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private async Task<string> ReadStringBytesAsync(int length, CancellationToken ct)
    {
        if (length == 0) return string.Empty;
        var buf = new byte[length];
        await ReadFullAsync(buf, ct);
        return Encoding.UTF8.GetString(buf);
    }

    private async Task ReadFullAsync(byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0) throw new EndOfStreamException("Connection closed by remote.");
            offset += read;
        }
    }
}
