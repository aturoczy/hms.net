namespace Hmsnet.Core.Models;

public class SortOrder
{
    public string Column { get; set; } = string.Empty;
    public SortDirection Order { get; set; } = SortDirection.Ascending;
}
