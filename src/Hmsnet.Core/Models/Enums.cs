namespace Hmsnet.Core.Models;

public enum PrincipalType { User, Role, Group }

public enum TableType { ManagedTable, ExternalTable, VirtualView, MaterializedView }

public enum FieldSchemaType
{
    // Primitive
    TinyInt, SmallInt, Int, BigInt,
    Float, Double, Decimal,
    Boolean,
    String, Varchar, Char,
    Binary,
    Timestamp, Date,
    // Complex
    Array, Map, Struct, UnionType
}

public enum SortDirection { Ascending = 1, Descending = 0 }

public enum StatisticsType { Boolean, Long, Double, String, Binary, Decimal, Date, Timestamp }
