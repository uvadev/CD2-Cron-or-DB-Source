using System;
using System.IO;
using System.Threading.Tasks;
using PullCanvasData2;
using PullCanvasData2.Structures;

namespace CD2_2_Tables;

public static class FetchFromCd2
{
    public static async Task GetData()
    {
        var all = Program.Config("tableList").ToString().Trim().Split(',');
        /*Console.Write(Program.AppConfig_GetKeyValue("client"));
        Console.Write(Program.AppConfig_GetKeyValue("key"));*/
        var api = new CanvasData(
            "https://api-gateway.instructure.com",
            Program.Config("client").ToString(),
            Program.Config("key").ToString()
        );

        await api.Authenticate();
        Console.WriteLine("Auth OK.");

        async Task TheThing(CanvasData canvasData,string tableName, string tableType)
        {
            var fType = tableType switch
            {
                "csv" => DataFormat.Csv,
                "parquet" => DataFormat.Parquet,
                "tsv" => DataFormat.Tsv,
                "json" => DataFormat.JsonLines,
                _ => throw new ArgumentOutOfRangeException(nameof(tableType), tableType, "<=== out of range: you done fucked up A-a-ron")
            };
            var job = await canvasData.PostSnapshotJob(tableName, fType);
            job = await canvasData.AwaitJobCompletion(job);

            var urls = await canvasData.GetJobUrls(job);
            Console.WriteLine($"{Program.Config("fullPathName").ToString().Trim()}\\download");
            Directory.CreateDirectory($"{Program.Config("fullPathName").ToString().Trim()}\\download");

            foreach (var url in urls)
            {
                var filePath = $"{Program.Config("fullPathName").ToString().Trim()}\\download\\{tableName}!{url.Key.Split('/')[1]}";
                Console.WriteLine(filePath);
                var downloadStream = await canvasData.StreamUrl(url.Value);

                using var fileStream = File.Create(filePath);
                await downloadStream.CopyToAsync(fileStream);
            }
        }
        
        foreach (var table in all)
        {
            await TheThing(api,table.Trim(), Program.Config("tableType").ToString().Trim());
            Console.WriteLine(table + " Completed");
        }
    }
}