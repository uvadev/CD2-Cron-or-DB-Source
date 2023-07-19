using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;

namespace CD2_2_Tables;

public static class CsvFilesToDb
{

    private static async Task<DataTable> GetSchema(SqlConnection connection, string tableName)
    {
        using var cmd = new SqlCommand($"SELECT TOP(0) * FROM {tableName}", connection);
        using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        return reader.GetSchemaTable();
    }

    /*private static Type NullableOf(Type t)
    {
        return t.IsValueType ? typeof(Nullable<>).MakeGenericType(t) : t;
    }*/
    
    public static async Task CsvToSql(string fileName)
    {
        var file = $"{Program.Config("fullPathName").ToString().Trim()}\\unzip\\{Path.GetFileName(fileName)}";
        var connection = new SqlConnection(Program.Config("connection").ToString());
        Console.WriteLine("=========================================\n");
        await connection.OpenAsync();
        Console.WriteLine("====================open=====================\n");

        List<dynamic> rows;
        List<string> columns;
        var reader = new StreamReader(file);
        var tableName =  $"dbo.{fileName.Split('!')[0]}";
        //Console.WriteLine(tableName);

        var schema = await GetSchema(connection, tableName);
        
        // (ColumnName, DataType)
        var typeMappings = schema
            .AsEnumerable()
            .Select(row => (row.Field<string>("ColumnName"), row.Field<Type>("DataType")))
            .ToDictionary(nt => nt.Item1, nt => nt.Item2);

        var sqlBulk = new SqlBulkCopy(connection);
        var csv = new CsvReader(reader,CultureInfo.CurrentCulture);
        {
            rows = csv.GetRecords<dynamic>().ToList();
            columns = csv.HeaderRecord?.ToList();
        }

        if (rows.Count == 0)
            return;

        var table = new DataTable();
        sqlBulk.ColumnMappings.Clear();

        if (columns != null)
        {
            foreach (var c in columns)
            {
                var colName = c.Replace(".", "_");
                Console.WriteLine($"columnName: {colName}");
                table.Columns.Add(colName, typeMappings[colName]);
                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping(colName, colName));
            }
        }

        foreach (var row in rows.Cast<IDictionary<string, object>>())//TODO: if null keep as null or re-write string.empty -> null : Implemented! 
        //TODO: Check performance
        {
            var rowValues = row.Select(rv =>
            {
                var colName = rv.Key.Replace(".", "_");
                return rv.Value is null or "" ? null : Convert.ChangeType(rv.Value, typeMappings[colName]);
            }).ToArray();

            table.Rows.Add(rowValues);
        }

        sqlBulk.DestinationTableName = tableName;

        sqlBulk.NotifyAfter = 1;
        //sqlBulk.EnableStreaming = true;
        sqlBulk.BulkCopyTimeout = 10000;
        sqlBulk.SqlRowsCopied += (sender, args) =>
        {
            Console.WriteLine($"Copied {args.RowsCopied} rows");
        };
        try
        {
            Console.WriteLine("Write To DB begun");
            await sqlBulk.WriteToServerAsync(table);
            csv.Dispose();
            sqlBulk.Close();
            Console.WriteLine("Done");
        }
        catch (Exception e)
        {
            if (!e.Message.Contains("Received an invalid column length from the bcp client for colid")) throw;
            const string pattern = @"\d+";
            var match = Regex.Match(e.Message, pattern);
            var index = Convert.ToInt32(match.Value) -1;

            var fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
            if (fi != null)
            {
                var sortedColumns = fi.GetValue(sqlBulk);
                var items = (object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(sortedColumns);

                if (items != null)
                {
                    var itemData = items[index].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    if (itemData != null)
                    {
                        var metadata = itemData.GetValue(items[index]);
                        var column = metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(metadata);
                        var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(metadata);
                        throw new FormatException($"Column: {column} contains data with a length greater than: {length}");
                    }
                }
            }
        }
    }
}