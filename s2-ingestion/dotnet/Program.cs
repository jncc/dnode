using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace dotnet
{
    class Program
    {
        // change from python version: remove unnecessary underscores which aren't accepted by .net regex engine 
        static string parsingRegex = @"Sentinel2([AB])_((20[0-9]{2})([0-9]{2})([0-9]{2}))\/SEN2_[0-9]{8}_lat([0-9]{2,4})lon([0-9]{2,4})_T([0-9]{2}[A-Z]{3})_ORB([0-9]{3})_(utm[0-9]{2}n)(_osgb)?_(clouds|sat|toposhad|valid|vmsk_sharp_rad_srefdem_stdsref|meta|thumbnail)(?!\.tif\.aux\.xml)";
        
        static void Main(string[] args)
        {
            Console.WriteLine("Hello!");

            var lines = File.ReadLines(@"../saved/list-20171206-101946.txt");

            // load the S3 objects from the local file dump
            var objects = from line in lines
                          let tokens = line.Split(' ')
                          select new {
                              key = tokens[0].ToString(),
                              size = tokens[1].ToString() // ToString will catch any nulls
                          };

            Console.WriteLine("{0} S3 objects in the input file.", objects.Count());

            // parse the objects as "assets" using the regex
            var assets = from o in objects
                         let match = Regex.Match(o.key, parsingRegex)
                         where match.Success
                         select ParseAsset(o.key, o.size, match);

            Console.WriteLine("{0} assets parsed.", assets.Count());
            
            // sanity check that ignored keys should be either S3 "directories", or xml or html files
            var ignored = from o in objects
                          let match = Regex.Match(o.key, parsingRegex)
                          where !match.Success
                          select o.key;

            Debug.Assert(ignored.All(key => key.EndsWith("/") || key.EndsWith(".xml") || key.EndsWith(".html")));

            // let's see which assets don't have a new projection
            var assetsWithoutNewProjection = assets.Where(a => a.new_projection == a.original_projection);
            Console.WriteLine("{0} assets without a new projection.", assetsWithoutNewProjection.Count());

            // why do these files not have a new projection?
            var nonRockallAssetsWithoutNewProjection = assetsWithoutNewProjection.Where(a => !a.s3_key.Contains("Rockall"));
            Console.WriteLine("{0} non-Rockall assets without a new projection.", nonRockallAssetsWithoutNewProjection.Count());
            nonRockallAssetsWithoutNewProjection.Select(a => a.s3_key).ToList().ForEach(Console.WriteLine);

            // group the assets into "products", ie things with a name, attributes and multiple associated files
            var products = from a in assets
                           let name = String.Format("S2{0}_{1}{2}{3}_lat{4}lon{5}_T{6}_ORB{7}_{8}{9}",
                                a.satellite_code, a.year, a.month, a.day, a.lat, a.lon, a.grid, a.orbit, a.original_projection,
                                a.new_projection != a.original_projection ? "_" + a.new_projection : "")
                           group a by name into g
                           select new Product {
                                Name = g.Key,
                                Files = (from a in g select new S3File {
                                            type = a.file_type,
                                            path = a.s3_key,
                                            size =  Utility.GetBytesReadable(long.Parse(a.s3_size)),
                                        }),
                                Attrs = g.First() // just use the first asset, all *should* be the same
                           };

            Console.WriteLine("{0} products parsed using name.", products.Count());

            // sanity-check grouping by Name string is correct
            var productsByKey = from p in assets
                                group p by new { p.satellite_code, p.year, p.month, p.day, p.lat, p.lon, p.grid, p.orbit,
                                    p.original_projection, p.new_projection };

            Console.WriteLine("{0} products parsed using GroupBy (should be same).", productsByKey.Count());
            Debug.Assert(products.Count() == productsByKey.Count());

            // do all products have a data file?
            var productsWithDataFile = products.Where(p => p.Files.Any(f => f.type == "data"));
            Console.WriteLine("{0} products have a data file.", productsWithDataFile.Count());

            // ok, how many files do these products actually have?
            var q = from p in productsWithDataFile
                    group p by p.Files.Count() into g
                    select new {
                        FileCount = g.Key,
                        ProductCount = g.Count(),
                        Products = g.AsEnumerable()
                    };

            // check that the products all have all 6 files associated with them
            Console.WriteLine("File counts for products with data files:");
            q.ToList().Select(x => new { x.FileCount, x.ProductCount }).ToList().ForEach(Console.WriteLine);

            Console.WriteLine("Products with fewer than 6 files:");
            (from x in q where x.FileCount < 6 from p in x.Products select p.Name).ToList().ForEach(Console.WriteLine);

            HtmlGenerator.GenerateByDate(productsWithDataFile);
        }

        static Asset ParseAsset(string key, string size, Match match)
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
                file_type=           match.Groups[12].Value == "vmsk_sharp_rad_srefdem_stdsref" ? "data" : match.Groups[12].Value,
            };
        }
    }



    // wish we had better type inference! these are just shapes which could be inferred.

    public class Asset
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
    
    public class Product
    {
        public string Name;
        public IEnumerable<S3File> Files;
        public Asset Attrs;
    }

    public class S3File
    {
        public string type;
        public string path;
        public string size;
    }        
}
