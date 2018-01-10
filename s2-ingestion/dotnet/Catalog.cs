using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dotnet
{
    public static class Catalog
    {
        static string outputDir = "../output/catalog";
        
        public static void GenerateJson(List<Product> products)
        {
            Directory.CreateDirectory(outputDir);
            Console.WriteLine("Generating catalog json...");

            // todo make the right output shape
            var output = products;

            string json = JsonConvert.SerializeObject(output, Formatting.Indented);
            File.WriteAllText(Path.Combine(outputDir, "catalog.json"), json);
        }
    }
}