using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Hmsnet.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddFeatureParity : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "CatalogId",
                table: "Databases",
                type: "integer",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "CatalogName",
                table: "Databases",
                type: "character varying(128)",
                maxLength: 128,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "DefaultValue",
                table: "Columns",
                type: "text",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "Catalogs",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                    Description = table.Column<string>(type: "text", nullable: true),
                    LocationUri = table.Column<string>(type: "character varying(4096)", maxLength: 4096, nullable: false),
                    Properties = table.Column<string>(type: "text", nullable: false),
                    CreateTime = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Catalogs", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "IcebergViews",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Name = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    DatabaseId = table.Column<int>(type: "integer", nullable: false),
                    MetadataLocation = table.Column<string>(type: "character varying(4096)", maxLength: 4096, nullable: false),
                    MetadataJson = table.Column<string>(type: "text", nullable: false),
                    CurrentVersionId = table.Column<int>(type: "integer", nullable: false),
                    CreateTime = table.Column<long>(type: "bigint", nullable: false),
                    LastAlteredTime = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IcebergViews", x => x.Id);
                    table.ForeignKey(
                        name: "FK_IcebergViews_Databases_DatabaseId",
                        column: x => x.DatabaseId,
                        principalTable: "Databases",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "NotificationEvents",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    EventTime = table.Column<long>(type: "bigint", nullable: false),
                    EventType = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                    CatalogName = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: true),
                    DbName = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: true),
                    TableName = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    MessageFormat = table.Column<string>(type: "character varying(32)", maxLength: 32, nullable: false),
                    Message = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_NotificationEvents", x => x.Id);
                });

            migrationBuilder.InsertData(
                table: "Catalogs",
                columns: new[] { "Id", "CreateTime", "Description", "LocationUri", "Name", "Properties" },
                values: new object[] { 1, 0L, "Default catalog (auto-created).", "hdfs:///user/hive/warehouse", "hive", "{}" });

            migrationBuilder.CreateIndex(
                name: "IX_Databases_CatalogId",
                table: "Databases",
                column: "CatalogId");

            migrationBuilder.CreateIndex(
                name: "IX_Catalogs_Name",
                table: "Catalogs",
                column: "Name",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_IcebergViews_DatabaseId_Name",
                table: "IcebergViews",
                columns: new[] { "DatabaseId", "Name" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_NotificationEvents_Id",
                table: "NotificationEvents",
                column: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_Databases_Catalogs_CatalogId",
                table: "Databases",
                column: "CatalogId",
                principalTable: "Catalogs",
                principalColumn: "Id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Databases_Catalogs_CatalogId",
                table: "Databases");

            migrationBuilder.DropTable(
                name: "Catalogs");

            migrationBuilder.DropTable(
                name: "IcebergViews");

            migrationBuilder.DropTable(
                name: "NotificationEvents");

            migrationBuilder.DropIndex(
                name: "IX_Databases_CatalogId",
                table: "Databases");

            migrationBuilder.DropColumn(
                name: "CatalogId",
                table: "Databases");

            migrationBuilder.DropColumn(
                name: "CatalogName",
                table: "Databases");

            migrationBuilder.DropColumn(
                name: "DefaultValue",
                table: "Columns");
        }
    }
}
