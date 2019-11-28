
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
        <!-- Google Tag Manager -->
        <script>
            (function (w, d, s, l, i) {
            w[l] = w[l] || []; w[l].push({'gtm.start': new Date().getTime(), event: 'gtm.js'}); 
            var f = d.getElementsByTagName(s)[0], j = d.createElement(s), dl = l != 'dataLayer' ? '&l=' + l : ''; 
            j.async = true; j.src ='https://www.googletagmanager.com/gtm.js?id=' + i + dl; f.parentNode.insertBefore(j, f);
            })(window, document, 'script', 'dataLayer', 'GTM-5TGXBJF');
        </script>
        <!-- End Google Tag Manager -->
        <title>Sentinel-2 ARD index by gridsquare</title>
        <link rel=""stylesheet"" href=""https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css"" />
        <link rel=""stylesheet"" href=""https://unpkg.com/leaflet@1.6.0/dist/leaflet.css""
            integrity=""sha512-xwE/Az9zrjBIphAcBb3F6JVqxf46+CDLwfLMHloNu6KEQCAWi6HcDUbeOfBIptF7tcCzusKFjFw2yuvEpDL9wQ==""
            crossorigin="""" />
        <script src=""https://unpkg.com/leaflet@1.6.0/dist/leaflet.js""
            integrity=""sha512-gZwIG9x3wUXg2hdXF6+rVkLF/0Vi9U8D2Ntg4Ga5I5BZpVkVxlJWbSQtXPSiUTtC0TjtGOmxa1AJPuV0CPthew==""
            crossorigin=""""></script>
        <script src=""grid.json.js"" type=""text/javascript""></script>
        <style>
            html,body {
                height: 100%;
            }
            #map {
                height: 800px;
                width: 100%;
            }
        </style>
    </head>
    <body>
        <!-- Google Tag Manager (noscript) -->
        <noscript><iframe src='https://www.googletagmanager.com/ns.html?id=GTM-5TGXBJF' height='0' width='0'
                style='display:none;visibility:hidden'></iframe></noscript>
        <!-- End Google Tag Manager (noscript) -->
        <div class=""ui grid container"">
        <br />
        <h1>Sentinel-2 ARD index by gridsquare</h1>
        <br />
        <div class=""ten wide column"">
            <div id=""map""></div>
        </div>
        <script>
            var url = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
            var map = L.map('map').setView([54.5, -4], 6);
            L.tileLayer(url, { maxZoom: 20 }).addTo(map);
            function onEachFeature(feature, layer) {

                var label = ""<a>"" + feature.properties.Name + ""</a>"";

                layer.bindTooltip(label, {
                    permanent: true,
                    interactive: true
                })
                layer.on('click', function (e) {
                    window.open(feature.properties.Name + '.html');
                });
                layer.on('mouseover', function () {
                    layer.setStyle({ color: '#ff3232' });
                });
                layer.on('mouseout', function () {
                    layer.setStyle({ color: '#66a5ff' });
                });
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

            s.Append("<html><head><!-- Google Tag Manager --><script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-5TGXBJF');</script><!-- End Google Tag Manager --><title></title><link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css\"/></head><body><!-- Google Tag Manager (noscript) --><noscript><iframe src='https://www.googletagmanager.com/ns.html?id=GTM-5TGXBJF' height='0' width='0' style='display:none;visibility:hidden'></iframe></noscript><!-- End Google Tag Manager (noscript) --><div class=\"ui container\">");
            s.Append("<br />");
            s.Append($"<h1>{gridsquare}</h1>");
            s.Append("<p><strong>Please note</strong> that all filenames have been updated to include the satellite code</p>");

            var sorted = from p in products
                         orderby p.Attrs.year, p.Attrs.month, p.Attrs.day
                         select p;

            HtmlProductList.Render(s, sorted);

            s.Append("</div></body></html>");

            File.WriteAllText(Path.Combine(outputDir, gridsquare + ".html"), s.ToString());
        }
    }    
}