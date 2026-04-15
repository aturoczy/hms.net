using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Hmsnet.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddIcebergMetadata : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "IcebergMetadata",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    HiveTableId = table.Column<int>(type: "integer", nullable: false),
                    MetadataLocation = table.Column<string>(type: "character varying(4096)", maxLength: 4096, nullable: false),
                    MetadataJson = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IcebergMetadata", x => x.Id);
                    table.ForeignKey(
                        name: "FK_IcebergMetadata_Tables_HiveTableId",
                        column: x => x.HiveTableId,
                        principalTable: "Tables",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_IcebergMetadata_HiveTableId",
                table: "IcebergMetadata",
                column: "HiveTableId",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "IcebergMetadata");
        }
    }
}
