using Hmsnet.Core.DTOs;
using Hmsnet.Core.Mapping;
using Hmsnet.Core.Models;

namespace Hmsnet.Tests.Features;

[TestClass]
public class ColumnDefaultsTests
{
    [TestMethod]
    public void ColumnDto_RoundTripsDefaultValue()
    {
        var col = new HiveColumn
        {
            Name = "created_at",
            TypeName = "timestamp",
            OrdinalPosition = 3,
            IsPartitionKey = false,
            DefaultValue = "CURRENT_TIMESTAMP"
        };

        var dto = col.ToDto();

        Assert.AreEqual("CURRENT_TIMESTAMP", dto.DefaultValue);

        var back = dto.ToModel();
        Assert.AreEqual("CURRENT_TIMESTAMP", back.DefaultValue);
    }

    [TestMethod]
    public void ColumnDto_DefaultValueOptional_AtBothEnds()
    {
        var col = new HiveColumn
        {
            Name = "name",
            TypeName = "string",
            OrdinalPosition = 0,
            IsPartitionKey = false,
        };

        var dto = col.ToDto();
        Assert.IsNull(dto.DefaultValue);

        var backwardsCompatible = new ColumnDto("legacy", "int", null, 0, false);
        Assert.IsNull(backwardsCompatible.DefaultValue);
    }
}
