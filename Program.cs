using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Nett;

namespace CD2_2_Tables
{
    internal static class Program
    {
        public static async Task Main()
        {
            await FetchFromCd2.GetData();
            await FileManagement.DecompressAllFiles(Config("fullPathName").ToString().Trim());
            if (Config("toDataBase").ToString().Trim() == "true")
            {
                await TruncateTables.TruncTables();
            }
            foreach (var file in Directory.
                         GetFiles($"{Config("fullPathName").ToString().Trim()}\\unzip", "*.csv",
                             SearchOption.TopDirectoryOnly))
            {
                Console.WriteLine($"Working on: {Path.GetFileName(file)}");
                if (Config("toDataBase").ToString().Trim() == "true" )
                {
                    await CsvFilesToDb.CsvToSql(Path.GetFileName(file));
                }
                Console.WriteLine($"Done with: {Path.GetFileName(file)}");
            }

            if (Config("purgeCsvFiles").ToString().Trim() == "true")
            {
                await Empty(new DirectoryInfo($"{Config("fullPathName").ToString().Trim()}\\unzip"),$"*.{Config("tableType").ToString().Trim()}");
            }

            if (Config("purgeTarballFiles").ToString().Trim() == "true")
            {
                await Empty(new DirectoryInfo($"{Config("fullPathName").ToString().Trim()}\\download"),"*.gz");
            }
        }

        public static TomlObject Config(string keyV)
        {
            var exePath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            var exeDir = Path.GetDirectoryName(exePath);
            TomlObject info = null;
            //Console.WriteLine(parentDirectory+"\\config.toml");
            {
                var parseToml = Toml.ReadFile(exeDir + "\\config.toml");
                var data = parseToml.Rows.AsQueryable().Where(x => x.Key == keyV);
                foreach (var keyValuePair in data)
                {
                    info = keyValuePair.Value;
                }
            }
            return info;
        }

        private static async Task Empty(DirectoryInfo directory, string ext)
        {
            await Task.Delay(1000);
            foreach(var file in directory.GetFiles($"{ext}")) file.Delete();
            Console.WriteLine($"Dumped {directory.Name}");
        }
    }
}