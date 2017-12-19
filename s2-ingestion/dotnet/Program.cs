using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace dotnet
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello!");

            var lines = File.ReadLines(@"./../saved/list-20171206-101946.txt");
            Console.WriteLine("{0} S3 keys in the input file.", lines.Count());

            // get the files that match the regex, parsed as an asset
            var assets = from line in lines
                         let tokens = line.Split(' ')
                         let key = tokens[0].ToString()
                         let size = tokens[1].ToString()
                         let match = Regex.Match(key, Parsing.ExtractionRegex)
                         where match.Success
                         select ParseS3Key(key, size, match);

            Console.WriteLine("{0} files parsed.", assets.Count());
            Console.WriteLine("{0} files with a new projection.", assets.Count(p => p.new_projection != p.original_projection));

            var products = from p in assets
                           let name = String.Format("S2{0}_{1}{2}{3}_lat{4}lon{5}_T{6}_ORB{7}_{8}{9}",
                               p.satellite_code, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit, p.original_projection,
                               p.new_projection != p.original_projection ? "_" + p.new_projection : "")
                           group p by name into g
                           select g;

            Console.WriteLine("{0} products parsed using name.", products.Count());

            var productsAgain = from p in assets
                                group p by new { p.satellite_code, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit,
                                    p.original_projection, p.new_projection } into g
                                select g;
            Console.WriteLine("{0} products parsed using GroupBy.", productsAgain.Count());
                                
            // products.ToList().ForEach(Console.WriteLine);
        }
    
        static Asset ParseS3Key(string key, string size, Match match)
        {
            return new Asset
            {
                s3_key=              key,
                s3_size=             size,
                satellite_code=      match.Groups[1].Value,
                satellite=           "sentinel-2" + match.Groups[1].Value.ToLower(),
                full_date=           match.Groups[2].Value,
                year=                match.Groups[3].Value,
                month=               match.Groups[4].Value,
                day=                 match.Groups[5].Value,
                lat=                 match.Groups[6].Value,
                lon=                 match.Groups[7].Value,
                grid=                match.Groups[8].Value,
                orbit=               match.Groups[9].Value,
                original_projection= match.Groups[10].Value,
                new_projection=      match.Groups[11].Success ? match.Groups[11].Value : match.Groups[10].Value,
                file_type=           match.Groups[12].Value,

            };
        }
    }

    class Asset
    {
        public string s3_key;
        public string s3_size;
        public string satellite_code;
        public string satellite;
        public string full_date;
        public string year;
        public string month;
        public string day;
        public string lat;
        public string lon;
        public string grid;
        public string orbit;
        public string original_projection;
        public string new_projection;
        public string file_type;
    }
}
