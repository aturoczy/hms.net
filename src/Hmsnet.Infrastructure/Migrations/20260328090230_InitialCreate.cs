using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Hmsnet.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Databases",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                    Description = table.Column<string>(type: "text", nullable: true),
                    LocationUri = table.Column<string>(type: "character varying(4096)", maxLength: 4096, nullable: false),
                    OwnerName = table.Column<string>(type: "text", nullable: true),
                    OwnerType = table.Column<int>(type: "integer", nullable: false),
                    Parameters = table.Column<string>(type: "text", nullable: false),
                    CreateTime = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Databases", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "StorageDescriptors",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Location = table.Column<string>(type: "character varying(4096)", maxLength: 4096, nullable: false),
                    InputFormat = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: false),
                    OutputFormat = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: false),
                    Compressed = table.Column<bool>(type: "boolean", nullable: false),
                    NumBuckets = table.Column<int>(type: "integer", nullable: false),
                    BucketColumns = table.Column<string>(type: "text", nullable: false),
                    SortColumns = table.Column<string>(type: "text", nullable: false),
                    Parameters = table.Column<string>(type: "text", nullable: false),
                    SkewedInfo = table.Column<string>(type: "text", nullable: false),
                    StoredAsSubDirectories = table.Column<bool>(type: "boolean", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_StorageDescriptors", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "SerDeInfos",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "text", nullable: true),
                    SerializationLib = table.Column<string>(type: "character varying(1024)", maxLength: 1024, nullable: false),
                    Parameters = table.Column<string>(type: "text", nullable: false),
                    StorageDescriptorId = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SerDeInfos", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SerDeInfos_StorageDescriptors_StorageDescriptorId",
                        column: x => x.StorageDescriptorId,
                        principalTable: "StorageDescriptors",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Tables",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    DatabaseId = table.Column<int>(type: "integer", nullable: false),
                    Owner = table.Column<string>(type: "text", nullable: true),
                    CreateTime = table.Column<long>(type: "bigint", nullable: false),
                    LastAccessTime = table.Column<long>(type: "bigint", nullable: false),
                    Retention = table.Column<int>(type: "integer", nullable: false),
                    TableType = table.Column<int>(type: "integer", nullable: false),
                    StorageDescriptorId = table.Column<int>(type: "integer", nullable: false),
                    ViewOriginalText = table.Column<string>(type: "text", nullable: true),
                    ViewExpandedText = table.Column<string>(type: "text", nullable: true),
                    Parameters = table.Column<string>(type: "text", nullable: false),
                    Temporary = table.Column<bool>(type: "boolean", nullable: false),
                    RewriteEnabled = table.Column<bool>(type: "boolean", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Tables", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Tables_Databases_DatabaseId",
                        column: x => x.DatabaseId,
                        principalTable: "Databases",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Tables_StorageDescriptors_StorageDescriptorId",
                        column: x => x.StorageDescriptorId,
                        principalTable: "StorageDescriptors",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Columns",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    TypeName = table.Column<string>(type: "character varying(2048)", maxLength: 2048, nullable: false),
                    Comment = table.Column<string>(type: "text", nullable: true),
                    OrdinalPosition = table.Column<int>(type: "integer", nullable: false),
                    TableId = table.Column<int>(type: "integer", nullable: false),
                    IsPartitionKey = table.Column<bool>(type: "boolean", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Columns", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Columns_Tables_TableId",
                        column: x => x.TableId,
                        principalTable: "Tables",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Partitions",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    TableId = table.Column<int>(type: "integer", nullable: false),
                    Values = table.Column<string>(type: "text", nullable: false),
                    CreateTime = table.Column<long>(type: "bigint", nullable: false),
                    LastAccessTime = table.Column<long>(type: "bigint", nullable: false),
                    StorageDescriptorId = table.Column<int>(type: "integer", nullable: false),
                    Parameters = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Partitions", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Partitions_StorageDescriptors_StorageDescriptorId",
                        column: x => x.StorageDescriptorId,
                        principalTable: "StorageDescriptors",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Partitions_Tables_TableId",
                        column: x => x.TableId,
                        principalTable: "Tables",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ColumnStatistics",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    ColumnName = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    ColumnType = table.Column<string>(type: "character varying(512)", maxLength: 512, nullable: false),
                    StatisticsType = table.Column<int>(type: "integer", nullable: false),
                    LastAnalyzed = table.Column<long>(type: "bigint", nullable: false),
                    NumNulls = table.Column<long>(type: "bigint", nullable: true),
                    NumDistinctValues = table.Column<long>(type: "bigint", nullable: true),
                    NumTrues = table.Column<long>(type: "bigint", nullable: true),
                    NumFalses = table.Column<long>(type: "bigint", nullable: true),
                    LongLow = table.Column<long>(type: "bigint", nullable: true),
                    LongHigh = table.Column<long>(type: "bigint", nullable: true),
                    DoubleLow = table.Column<double>(type: "double precision", nullable: true),
                    DoubleHigh = table.Column<double>(type: "double precision", nullable: true),
                    MaxColLen = table.Column<long>(type: "bigint", nullable: true),
                    AvgColLen = table.Column<double>(type: "double precision", nullable: true),
                    DecimalLow = table.Column<string>(type: "text", nullable: true),
                    DecimalHigh = table.Column<string>(type: "text", nullable: true),
                    DateLow = table.Column<long>(type: "bigint", nullable: true),
                    DateHigh = table.Column<long>(type: "bigint", nullable: true),
                    BitVector = table.Column<string>(type: "text", nullable: true),
                    TableId = table.Column<int>(type: "integer", nullable: true),
                    PartitionId = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ColumnStatistics", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ColumnStatistics_Partitions_PartitionId",
                        column: x => x.PartitionId,
                        principalTable: "Partitions",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ColumnStatistics_Tables_TableId",
                        column: x => x.TableId,
                        principalTable: "Tables",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Columns_TableId",
                table: "Columns",
                column: "TableId");

            migrationBuilder.CreateIndex(
                name: "IX_ColumnStatistics_PartitionId",
                table: "ColumnStatistics",
                column: "PartitionId");

            migrationBuilder.CreateIndex(
                name: "IX_ColumnStatistics_TableId",
                table: "ColumnStatistics",
                column: "TableId");

            migrationBuilder.CreateIndex(
                name: "IX_Databases_Name",
                table: "Databases",
                column: "Name",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Partitions_StorageDescriptorId",
                table: "Partitions",
                column: "StorageDescriptorId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Partitions_TableId",
                table: "Partitions",
                column: "TableId");

            migrationBuilder.CreateIndex(
                name: "IX_SerDeInfos_StorageDescriptorId",
                table: "SerDeInfos",
                column: "StorageDescriptorId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Tables_DatabaseId_Name",
                table: "Tables",
                columns: new[] { "DatabaseId", "Name" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Tables_StorageDescriptorId",
                table: "Tables",
                column: "StorageDescriptorId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Columns");

            migrationBuilder.DropTable(
                name: "ColumnStatistics");

            migrationBuilder.DropTable(
                name: "SerDeInfos");

            migrationBuilder.DropTable(
                name: "Partitions");

            migrationBuilder.DropTable(
                name: "Tables");

            migrationBuilder.DropTable(
                name: "Databases");

            migrationBuilder.DropTable(
                name: "StorageDescriptors");
        }
    }
}
