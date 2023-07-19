using System;
using System.Collections;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace CD2_2_Tables;

public static class TruncateTables
{
    public static async Task TruncTables()
    {
        var connection = new SqlConnection(Program.Config("connection").ToString());
        Console.WriteLine("=========================================\n");
        await connection.OpenAsync();
        Console.WriteLine("====================time to trunc=====================\n");

        foreach (var table in Program.Config("tableList").ToString().Trim().Split(','))
        {
            using var cmd = new SqlCommand($"TRUNCATE TABLE {table.Trim()}", connection);
            cmd.ExecuteNonQuery();
        }
    }
}