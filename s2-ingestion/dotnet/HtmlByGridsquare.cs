
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
            
            var s = new StringBuilder();

            s.Append(@"<html>
                        <head>
                        <title>Sentinel 2 ARD Index</title>
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
                        <h1>Sentinel-2 ARD Index (by gridsquare)</h1>
                        <table width=""80%"">
                        <thead>
                        <td>Gridsquare</td>
                        </thead>
                        <tbody>");

            var productsByGridsquare = from p in products
                                       group p by p.Attrs.grid into g
                                       orderby g.Key
                                       select g;

            foreach (var productsInGridsquare in productsByGridsquare)
            {
                string gridsquare = productsInGridsquare.Key;
                
                // (no real reason to use a table in this case)
                s.Append("<tr>");
                s.Append("<td>");
                s.Append($"<a href=\"{gridsquare}.html\">{gridsquare}</a> ");
                s.Append($"<span class=\"ui tiny \">{productsInGridsquare.Count()} products</span><br/>");
                s.Append("</td>");
                s.Append("</tr>");

                GenerateGridsquarePage(gridsquare, productsInGridsquare);
            }

            s.Append("</tbody></table></div></body></html>");

            Directory.CreateDirectory(outputDir);
            File.WriteAllText(Path.Combine(outputDir, "index.html"), s.ToString());
        }

        static void GenerateGridsquarePage(string gridsquare, IEnumerable<Product> products)
        {
            var s = new StringBuilder();

            s.Append("<html><head><title></title><link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css\"/></head><body><div class=\"ui container\">");
            s.Append($"<h1>{gridsquare}</h1>");

            var sorted = from p in products
                         orderby p.Attrs.year, p.Attrs.month, p.Attrs.day
                         select p;

            HtmlProductList.Generate(s, sorted);

            s.Append("</div></body></html>");

            File.WriteAllText(Path.Combine(outputDir, gridsquare + ".html"), s.ToString());
        }
    }    
}