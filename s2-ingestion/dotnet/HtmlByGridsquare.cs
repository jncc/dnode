
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
        static string outputDir = "../output/web/bygridsquare";

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

            // filter feature gridsquares to only those with products, remove extraneous properties, add the product count
            var output = new {
                type = "FeatureCollection",
                name = "s2grid",
                features = (from f in geojson["features"].Children()
                            let gridsquare = f["properties"]["Name"].ToString()
                            let productCount = (from grouping in productsByGridsquare
                                                where grouping.Key == gridsquare
                                                from product in grouping
                                                select product).Count()
                            where productCount > 0
                            select new {
                                type = "Feature",
                                geometry = f["geometry"],
                                properties = new { Name = gridsquare, ProductCount = productCount },
                            }).ToArray()
            };

            string js = $"var grid = {JsonConvert.SerializeObject(output)};";
            File.WriteAllText(Path.Combine(outputDir, "grid.json.js"), js);
        }

        static void GenerateHtmlPages(List<IGrouping<string, Product>> productsByGridsquare)
        {
            var s = new StringBuilder();

            s.Append(@"<html>
                        <head>
                        <title>Sentinel-2 ARD index by gridsquare</title>
                        <link rel=""stylesheet"" href=""https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css""/>
                        <link rel=""stylesheet"" href=""https://unpkg.com/leaflet@1.1.0/dist/leaflet.css""
                            integrity=""sha512-wcw6ts8Anuw10Mzh9Ytw4pylW8+NAD4ch3lqm9lzAsTxg0GFeJgoAtxuCLREZSC5lUXdVyo/7yfsqFjQ4S+aKw==""
                            crossorigin=""""/>
                        <script src=""https://unpkg.com/leaflet@1.1.0/dist/leaflet.js""
                            integrity=""sha512-mNqn2Wg7tSToJhvHcqfzLMU6J4mkOImSPTxVZAdo+lcPlk+GhZmYgACEe0x35K7YzW1zJ7XyJV/TT1MrdXvMcA==""
                            crossorigin=""""></script>
                        <script src=""grid.json.js"" type=""text/javascript""></script>
                        <style>
                            html, body { height: 100%; }
                            #map { height: 90%; width:100%; }
                        </style>
                        </head>
                        <body>
                        <div class=""ui grid container"">
                        <br />
                        <h1>Sentinel-2 ARD index by gridsquare</h1>
                        <div class=""ten wide column"">
                            <div id=""map""></div>
                        </div>
                        <script>
                            var url = 'https://api.mapbox.com/styles/v1/sumothecat/cj5w3i6w672u12slb33spg3te/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1Ijoic3Vtb3RoZWNhdCIsImEiOiJjaWxocngyanYwMDY4dmprcTg4ODN2Z3B2In0.CockfZdHAzqOfsbw8VcQyQ';
                            var map = L.map('map').setView([54.5, -4], 5);
                            L.tileLayer(url, { maxZoom: 20 }).addTo(map);
                            function onEachFeature(feature, layer) {
                                layer.on('click', function(e) {
                                    window.open(feature.properties.Name + '.html');
                                });

                                layer.bindTooltip(feature.properties.Name, {
                                    permanent: true
                                })
                            }
                            L.geoJSON(grid, { onEachFeature: onEachFeature }).addTo(map);
                        </script>
                        <div class=""six wide column"">
                        ");


            foreach (var productsInGridsquare in productsByGridsquare)
            {
                string gridsquare = productsInGridsquare.Key;
                
                s.Append($"<a href=\"{gridsquare}.html\">{gridsquare}</a> ");
                s.Append($"<span style=\"color:#999\">  {productsInGridsquare.Count()} products</span><br/>");
                s.Append("<hr />");                

                GenerateGridsquarePage(gridsquare, productsInGridsquare);
            }

            s.Append("</div></div></body></html>");

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