using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace grid
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello!");

            string json = File.ReadAllText(@"./s2ukwidegrid.json");
            var gridsquares = JObject.Parse(json)["features"].Children();
            Console.WriteLine($"{gridsquares.Count()} gridsquares in input file.");
            var names = gridsquares.Select(s => s["properties"]["Name"]);
            names.ToList().ForEach(Console.WriteLine);
        }
    }
}
