
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace dotnet
{
    public class HtmlByGridsquare
    {
        static string outputDir = "../output/bygridsquare";

        public static void Generate(IEnumerable<Product> products)
        {
            Console.WriteLine("Generating HTML by gridsquare...");
            Directory.CreateDirectory(outputDir);
            
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

            var productsByGridsquare = from p in products
                                       group p by p.Attrs.grid into g
                                       orderby g.Key
                                       select g;

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