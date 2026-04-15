using System.Text.Json;
using Hmsnet.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Hmsnet.Infrastructure.Data;

public class MetastoreDbContext(DbContextOptions<MetastoreDbContext> options) : DbContext(options)
{
    public DbSet<HiveDatabase> Databases => Set<HiveDatabase>();
    public DbSet<HiveTable> Tables => Set<HiveTable>();
    public DbSet<HiveColumn> Columns => Set<HiveColumn>();
    public DbSet<HivePartition> Partitions => Set<HivePartition>();
    public DbSet<StorageDescriptor> StorageDescriptors => Set<StorageDescriptor>();
    public DbSet<SerDeInfo> SerDeInfos => Set<SerDeInfo>();
    public DbSet<ColumnStatistics> ColumnStatistics => Set<ColumnStatistics>();
    public DbSet<IcebergTableMetadata> IcebergMetadata => Set<IcebergTableMetadata>();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        var jsonOptions = new JsonSerializerOptions();

        // ── Converters ────────────────────────────────────────────────────────

        var dictConverter = new ValueConverter<Dictionary<string, string>, string>(
            v => JsonSerializer.Serialize(v, jsonOptions),
            v => JsonSerializer.Deserialize<Dictionary<string, string>>(v, jsonOptions) ?? new Dictionary<string, string>());

        var dictComparer = new ValueComparer<Dictionary<string, string>>(
            (a, b) => JsonSerializer.Serialize(a, jsonOptions) == JsonSerializer.Serialize(b, jsonOptions),
            v => JsonSerializer.Serialize(v, jsonOptions).GetHashCode(),
            v => JsonSerializer.Deserialize<Dictionary<string, string>>(JsonSerializer.Serialize(v, jsonOptions), jsonOptions)!);

        var stringListConverter = new ValueConverter<List<string>, string>(
            v => JsonSerializer.Serialize(v, jsonOptions),
            v => JsonSerializer.Deserialize<List<string>>(v, jsonOptions) ?? new List<string>());

        var stringListComparer = new ValueComparer<List<string>>(
            (a, b) => JsonSerializer.Serialize(a, jsonOptions) == JsonSerializer.Serialize(b, jsonOptions),
            v => JsonSerializer.Serialize(v, jsonOptions).GetHashCode(),
            v => JsonSerializer.Deserialize<List<string>>(JsonSerializer.Serialize(v, jsonOptions), jsonOptions)!);

        var sortOrderConverter = new ValueConverter<List<SortOrder>, string>(
            v => JsonSerializer.Serialize(v, jsonOptions),
            v => JsonSerializer.Deserialize<List<SortOrder>>(v, jsonOptions) ?? new List<SortOrder>());

        var sortOrderComparer = new ValueComparer<List<SortOrder>>(
            (a, b) => JsonSerializer.Serialize(a, jsonOptions) == JsonSerializer.Serialize(b, jsonOptions),
            v => JsonSerializer.Serialize(v, jsonOptions).GetHashCode(),
            v => JsonSerializer.Deserialize<List<SortOrder>>(JsonSerializer.Serialize(v, jsonOptions), jsonOptions)!);

        var skewedInfoConverter = new ValueConverter<SkewedInfo, string>(
            v => JsonSerializer.Serialize(v, jsonOptions),
            v => JsonSerializer.Deserialize<SkewedInfo>(v, jsonOptions) ?? new SkewedInfo());

        // ── HiveDatabase ──────────────────────────────────────────────────────
        mb.Entity<HiveDatabase>(e =>
        {
            e.HasKey(d => d.Id);
            e.HasIndex(d => d.Name).IsUnique();
            e.Property(d => d.Name).HasMaxLength(128).IsRequired();
            e.Property(d => d.LocationUri).HasMaxLength(4096);
            e.Property(d => d.Parameters)
                .HasConversion(dictConverter)
                .Metadata.SetValueComparer(dictComparer);
        });

        // ── HiveTable ─────────────────────────────────────────────────────────
        mb.Entity<HiveTable>(e =>
        {
            e.HasKey(t => t.Id);
            e.HasIndex(t => new { t.DatabaseId, t.Name }).IsUnique();
            e.Property(t => t.Name).HasMaxLength(256).IsRequired();
            e.Property(t => t.Parameters)
                .HasConversion(dictConverter)
                .Metadata.SetValueComparer(dictComparer);

            e.HasOne(t => t.Database)
                .WithMany(d => d.Tables)
                .HasForeignKey(t => t.DatabaseId)
                .OnDelete(DeleteBehavior.Cascade);

            e.HasOne(t => t.StorageDescriptor)
                .WithOne(sd => sd.Table)
                .HasForeignKey<HiveTable>(t => t.StorageDescriptorId)
                .OnDelete(DeleteBehavior.Cascade);

            e.HasMany(t => t.Columns)
                .WithOne(c => c.Table)
                .HasForeignKey(c => c.TableId)
                .OnDelete(DeleteBehavior.Cascade);

            e.Ignore(t => t.PartitionKeys);
        });

        // ── HiveColumn ────────────────────────────────────────────────────────
        mb.Entity<HiveColumn>(e =>
        {
            e.HasKey(c => c.Id);
            e.Property(c => c.Name).HasMaxLength(256).IsRequired();
            e.Property(c => c.TypeName).HasMaxLength(2048).IsRequired();
        });

        mb.Entity<HiveColumn>().Navigation(c => c.Table).AutoInclude(false);

        // ── StorageDescriptor ─────────────────────────────────────────────────
        mb.Entity<StorageDescriptor>(e =>
        {
            e.HasKey(sd => sd.Id);
            e.Property(sd => sd.Location).HasMaxLength(4096);
            e.Property(sd => sd.InputFormat).HasMaxLength(1024);
            e.Property(sd => sd.OutputFormat).HasMaxLength(1024);
            e.Property(sd => sd.Parameters)
                .HasConversion(dictConverter)
                .Metadata.SetValueComparer(dictComparer);
            e.Property(sd => sd.BucketColumns)
                .HasConversion(stringListConverter)
                .Metadata.SetValueComparer(stringListComparer);
            e.Property(sd => sd.SortColumns)
                .HasConversion(sortOrderConverter)
                .Metadata.SetValueComparer(sortOrderComparer);
            e.Property(sd => sd.SkewedInfo)
                .HasConversion(skewedInfoConverter);

            e.HasOne(sd => sd.SerDeInfo)
                .WithOne(s => s.StorageDescriptor)
                .HasForeignKey<SerDeInfo>(s => s.StorageDescriptorId)
                .OnDelete(DeleteBehavior.Cascade);

            e.Ignore(sd => sd.Table);
            e.Ignore(sd => sd.Partition);
        });

        // ── SerDeInfo ─────────────────────────────────────────────────────────
        mb.Entity<SerDeInfo>(e =>
        {
            e.HasKey(s => s.Id);
            e.Property(s => s.SerializationLib).HasMaxLength(1024).IsRequired();
            e.Property(s => s.Parameters)
                .HasConversion(dictConverter)
                .Metadata.SetValueComparer(dictComparer);
        });

        // ── HivePartition ─────────────────────────────────────────────────────
        mb.Entity<HivePartition>(e =>
        {
            e.HasKey(p => p.Id);
            // ValuesJson is the persisted column; Values is a computed property
            e.Property(p => p.ValuesJson).HasColumnName("Values").IsRequired();
            e.Ignore(p => p.Values);
            e.Property(p => p.Parameters)
                .HasConversion(dictConverter)
                .Metadata.SetValueComparer(dictComparer);

            e.HasOne(p => p.Table)
                .WithMany(t => t.Partitions)
                .HasForeignKey(p => p.TableId)
                .OnDelete(DeleteBehavior.Cascade);

            e.HasOne(p => p.StorageDescriptor)
                .WithOne(sd => sd.Partition)
                .HasForeignKey<HivePartition>(p => p.StorageDescriptorId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        // ── IcebergTableMetadata ──────────────────────────────────────────────
        mb.Entity<IcebergTableMetadata>(e =>
        {
            e.HasKey(m => m.Id);
            e.HasIndex(m => m.HiveTableId).IsUnique();
            e.Property(m => m.MetadataLocation).HasMaxLength(4096).IsRequired();
            e.Property(m => m.MetadataJson).IsRequired();

            e.HasOne(m => m.HiveTable)
                .WithOne()
                .HasForeignKey<IcebergTableMetadata>(m => m.HiveTableId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        // ── ColumnStatistics ──────────────────────────────────────────────────
        mb.Entity<ColumnStatistics>(e =>
        {
            e.HasKey(cs => cs.Id);
            e.Property(cs => cs.ColumnName).HasMaxLength(256).IsRequired();
            e.Property(cs => cs.ColumnType).HasMaxLength(512).IsRequired();

            e.HasOne(cs => cs.Table)
                .WithMany()
                .HasForeignKey(cs => cs.TableId)
                .OnDelete(DeleteBehavior.Cascade)
                .IsRequired(false);

            e.HasOne(cs => cs.Partition)
                .WithMany(p => p.ColumnStatistics)
                .HasForeignKey(cs => cs.PartitionId)
                .OnDelete(DeleteBehavior.Cascade)
                .IsRequired(false);
        });
    }
}
