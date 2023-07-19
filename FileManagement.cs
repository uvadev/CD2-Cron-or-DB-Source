using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace CD2_2_Tables;

public static class FileManagement
{
    private static async Task DecompressFile(string compressedFileName, string decompressedFileName)
    {
        using var compressedFileStream = File.Open(compressedFileName, FileMode.Open);
        using var outputFileStream = File.Create(decompressedFileName);
        using var decompressor = new GZipStream(compressedFileStream, CompressionMode.Decompress);
        await decompressor.CopyToAsync(outputFileStream);
    }

    public static async Task DecompressAllFiles(string currentLocation)
    {
        Directory.CreateDirectory($"{currentLocation}//unzip");
        var filePaths = Directory.GetFiles($"{currentLocation}\\download", "*.gz",
            SearchOption.TopDirectoryOnly);
        //Console.Write(filePaths.ToPrettyString());
        foreach (var file in filePaths)
        {
            await DecompressFile(file, $"{currentLocation}\\unzip\\{Path.GetFileNameWithoutExtension(file)}");
        }
    }
}