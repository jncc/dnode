
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dotnet
{
    public class HtmlByGridsquare
    {
        static string outputDir = "../output/bygridsquare";

        public static void Generate(IEnumerable<Product> products)
        {
            Console.WriteLine("Generating HTML by gridsquare...");
            Directory.CreateDirectory(outputDir);

            var productsByGridsquare = (from p in products
                                        group p by p.Attrs.grid into g
                                        orderby g.Key
                                        select g).ToList();

            GenerateGridGeojson(productsByGridsquare);
            GenerateHtmlPages(productsByGridsquare);
        }

        static void GenerateGridGeojson(List<IGrouping<string, Product>> productsByGridsquare)
        {
            var gridsquaresWithProducts = productsByGridsquare.Select(grouping => grouping.Key).ToList();
            Console.WriteLine($"{gridsquaresWithProducts.Count()} gridsquares with products in them.");
            
            var geojson = JObject.Parse(File.ReadAllText(@"../grid/s2ukwidegrid.json"));

            var gridsquaresInInputMap = (from f in geojson["features"].Children()
                                         let gridsquare = f["properties"]["Name"].ToString()
                                         select gridsquare).ToList();

            Console.WriteLine($"{gridsquaresInInputMap.Count} gridsquare features in input file.");

            // ensure we have a feature gridsquare for every product-containing gridsquare
            Debug.Assert( gridsquaresWithProducts.All(s => gridsquaresInInputMap.Contains(s)) );

            // filter feature gridsquares to only those with products, and remove extraneous properties
            var output = new {
                type = "FeatureCollection",
                name = "s2grid",
                features = (from f in geojson["features"].Children()
                            let gridsquare = f["properties"]["Name"].ToString()
                            where gridsquaresWithProducts.Contains(gridsquare)
                            select new {
                                type = "Feature",
                                geometry = f["geometry"],
                                properties = new {
                                    Name = gridsquare,
                                    ProductCount = productsByGridsquare.Single(grouping => grouping.Key == gridsquare).Count()
                                },
                            }).ToArray()
            };

            File.WriteAllText(Path.Combine(outputDir, "grid.json"), JsonConvert.SerializeObject(output));
        }

        static void GenerateHtmlPages(List<IGrouping<string, Product>> productsByGridsquare)
        {
            var s = new StringBuilder();

            s.Append(@"<html>
                        <head>
                        <title>Sentinel-2 ARD index by gridsquare</title>
                        <link rel=""stylesheet"" href=""https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css""/>
                        <style>
                            td {
                                border-bottom: 1px solid #bfc1c3;
                                padding-bottom: 0.5em;
                                text-align: left;
                            }
                            tr {
                                border-bottom: 1px solid #bfc1c3;
                                margin-bottom: 1em;
                            }
                        </style>        
                        </head>
                        <body>
                        <div class=""ui container"">
                        <br />
                        <h1>Sentinel-2 ARD index by gridsquare</h1>");


            foreach (var productsInGridsquare in productsByGridsquare)
            {
                string gridsquare = productsInGridsquare.Key;
                
                s.Append($"<a href=\"{gridsquare}.html\">{gridsquare}</a> ");
                s.Append($"<span style=\"color:#999\">  {productsInGridsquare.Count()} products</span><br/>");
                s.Append("<hr />");                

                GenerateGridsquarePage(gridsquare, productsInGridsquare);
            }

            s.Append("</div></body></html>");

            File.WriteAllText(Path.Combine(outputDir, "index.html"), s.ToString());
        }

        static void GenerateGridsquarePage(string gridsquare, IEnumerable<Product> products)
        {
            var s = new StringBuilder();

            s.Append("<html><head><title></title><link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css\"/></head><body><div class=\"ui container\">");
            s.Append("<br />");
            s.Append($"<h1>{gridsquare}</h1>");

            var sorted = from p in products
                         orderby p.Attrs.year, p.Attrs.month, p.Attrs.day
                         select p;

            HtmlProductList.Render(s, sorted);

            s.Append("</div></body></html>");

            File.WriteAllText(Path.Combine(outputDir, gridsquare + ".html"), s.ToString());
        }
    }    
}