namespace Hmsnet.Core.Models;

public class SkewedInfo
{
    /// <summary>Skewed column names.</summary>
    public List<string> SkewedColNames { get; set; } = [];

    /// <summary>Skewed column values (each inner list is one skew value tuple).</summary>
    public List<List<string>> SkewedColValues { get; set; } = [];

    /// <summary>Map from serialized skew values to their HDFS location.</summary>
    public Dictionary<string, string> SkewedColValueLocationMaps { get; set; } = [];
}
