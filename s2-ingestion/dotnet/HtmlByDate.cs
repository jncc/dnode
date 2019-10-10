
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace dotnet
{
    public static class HtmlByDate
    {
        static string outputDir = "../output/web/bydate";
        
        public static void Generate(IEnumerable<Product> products)
        {
            Console.WriteLine("Generating HTML by date...");

            var s = new StringBuilder();

            s.Append(@"<html>
                        <head>
                        <!-- Google Tag Manager -->
                        <script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
                        new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
                        j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
                        'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
                        })(window,document,'script','dataLayer','GTM-5TGXBJF');</script>
                        <!-- End Google Tag Manager -->
                        <title>Sentinel-2 ARD index by date</title>
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
                        <!-- Google Tag Manager (noscript) -->
                        <noscript><iframe src='https://www.googletagmanager.com/ns.html?id=GTM-5TGXBJF'
                        height='0' width='0' style='display:none;visibility:hidden'></iframe></noscript>
                        <!-- End Google Tag Manager (noscript) -->
                        <div class=""ui container"">
                        <br />
                        <h1>Sentinel-2 ARD index by date</h1>
                        <br />
                        <table width=""80%"">
                        <thead>
                        <td>Year</td>
                        <td>Month</td>
                        </thead>
                        <tbody>");

            var byYear = from p in products
                         orderby p.Attrs.year
                         group p by p.Attrs.year;

            foreach (var productsInYear in byYear)
            {
                string year = productsInYear.Key;
                
                s.Append("<tr>");
                s.Append("<td width=\"25%\">" + year + "</td>");
                s.Append("<td>");

                var byMonth = from p in productsInYear
                              group p by p.Attrs.month into g
                              orderby g.Key
                              select g;

                foreach (var productsInMonth in byMonth)
                {
                    string month = productsInMonth.Key;
                    string monthName = CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(int.Parse(month)); 
                    
                    s.Append("<div>");
                    s.Append($"<a href=\"{year}/{month}.html\">{monthName}</a>");
                    s.Append($"<span style=\"color:#aaa; \"> &nbsp; {productsInMonth.Count()} products</span><br/>");
                    s.Append("</div>");

                    GenerateMonthPage(productsInMonth, year, monthName);
                }

                s.Append("</td>");
                s.Append("</tr>");                
            }

            s.Append("</tbody></table></div></body></html>");

            Directory.CreateDirectory(outputDir);
            File.WriteAllText(Path.Combine(outputDir, "index.html"), s.ToString());
        }


        static void GenerateMonthPage(IGrouping<string, Product> productsInMonth, string year, string monthName)
        {
            var s = new StringBuilder();
            string month = productsInMonth.Key;

            s.Append("<html><head><!-- Google Tag Manager --><script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-5TGXBJF');</script><!-- End Google Tag Manager --><title></title><link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css\"/></head><body><!-- Google Tag Manager (noscript) --><noscript><iframe src='https://www.googletagmanager.com/ns.html?id=GTM-5TGXBJF' height='0' width='0' style='display:none;visibility:hidden'></iframe></noscript><!-- End Google Tag Manager (noscript) --><div class=\"ui container\">");
            s.Append("<br />");
            s.Append($"<h1>{monthName} {year}</h1>");
            s.Append("<p><strong>Please note</strong> that all filenames have been updated to include the satellite code</p>");

            var byGridsquare = from p in productsInMonth
                               group p by p.Attrs.grid into g
                               orderby g.Key
                               select g;

            foreach (var productsInGridsquare in byGridsquare)
            {
                string gridsquare = productsInGridsquare.Key;
                s.Append($"<h2>{gridsquare}</h2>");

                HtmlProductList.Render(s, productsInGridsquare);
            }

            s.Append("</div></body></html>");

            string dir = Path.Combine(outputDir, year);
            Directory.CreateDirectory(dir);            
            File.WriteAllText(Path.Combine(dir, month + ".html"), s.ToString());
        }
    }
}
