
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace dotnet
{
    public static class HtmlGenerator
    {
        static string outputDir = "../output/bydate";
        static string s3BasePath = "https://s3-eu-west-1.amazonaws.com/eocoe-sentinel-2/";
        
        public static void GenerateByDate(IEnumerable<Product> products)
        {
            Console.WriteLine("Generating HTML by date...");
                   
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
                        <h1>Sentinel-2 ARD Index</h1>
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
                    s.AppendFormat("<a href=\"{0}/{1}.html\">{2}</a> ", year, month, monthName);
                    s.AppendFormat("<span class=\"ui tiny \">{0} products</span><br/>", productsInMonth.Count());
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

            s.Append("<html><head><title></title><link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css\"/></head><body><div class=\"ui container\">");
            s.AppendFormat("<h1>{0} {1}</h1>", year, monthName);

            var byGridsquare = from p in productsInMonth
                               group p by p.Attrs.grid into g
                               orderby g.Key
                               select g;

            var ordered = from p in productsInMonth
                          orderby p.Attrs.day, p.Attrs.grid
                          select p;

            foreach (var productsInGridsquare in byGridsquare)
            {
                string gridsquare = productsInGridsquare.Key;
                
                s.AppendFormat("<h2>{0}</h2>", gridsquare);
//              s.AppendFormat("<div>{0} products</div>", productsInGridsquare.Count());
                s.Append("<div class=\"ui items\">");

                foreach (var p in productsInGridsquare)
                {
                    var dataFile = p.Files.Single(f => f.type == "data");
                    string thumbnailPath = "thumbnails/" + Path.GetFileName(dataFile.path.Replace("_vmsk_sharp_rad_srefdem_stdsref.tif", "_thumbnail.jpg"));

                    Action<string, string, string> renderFile = (name, type, filetype) =>
                    {
                        var file = p.Files.SingleOrDefault(f => f.type == type);
                        if (file == null) {
                            s.Append($"<div>No {name} available</div>");
                        } else {
                            string path = s3BasePath + file.path;
                            s.Append($"<div><a href=\"{path}\">{name}</a> {filetype} {file.size}</div>");
                        }
                    };

                    s.Append("<div class=\"item\">");
                    s.Append("<div class=\"image\">");
                    s.AppendFormat("<img src=\"{0}\" height=\"100px\" width=\"100px\">", s3BasePath + thumbnailPath);
                    s.Append("</div>");
                    s.Append("<div class=\"content\">");
                    s.AppendFormat("<div class=\"header\">{0}</div>", p.Name);
                    s.Append("<div class=\"meta\">");
                    s.AppendFormat("<span>{0} &bullet; {1}</span>", p.Attrs.day + " " + monthName, gridsquare);
                    s.Append("</div>");
                    // s.Append("<div class=\"description\">");
                    // s.AppendFormat("<p>{0}</p>", "blah");
                    // s.Append("</div>");
                    s.Append("<div class=\"extra\">");

                    // s.AppendFormat("<div><a href=\"{0}\">Data file</a> Geotiff {1}</div>", s3BasePath + dataFile.path, dataFile.size);
                    
                    renderFile("Data file", "data", "Geotiff");
                    renderFile("Cloud file", "clouds", "Geotiff");
                    renderFile("Saturated pixel mask", "sat", "Geotiff");
                                        
                    
                    //s.Append("<ul>");
                    // foreach (var f in p.Files)
                    // {
                    //     s.Append("<li>");
                    //     s.AppendFormat("{0} {1}<br />", f.type, f.path);
                    //     s.Append("</li>");
                    // }
                    // s.Append("</ul>");
                    s.Append("</div>"); // extra
                    s.Append("</div>"); // content
                    s.Append("</div>"); // item
                }

                s.Append("</div>"); // ui items
            }

    //                         p = data[year][month][day][grid] # the product at this gridsquare
    //                         product_file = next((f for f in p['files'] if f['type']=='product'), None)
    //                         clouds_file = next((f for f in p['files'] if f['type']=='clouds'), None)
    //                         meta_file = next((f for f in p['files'] if f['type']=='meta'), None)
    //                         sat_file = next((f for f in p['files'] if f['type']=='sat'), None)
    //                         toposhad_file = next((f for f in p['files'] if f['type']=='toposhad'), None)
    //                         valid_file = next((f for f in p['files'] if f['type']=='valid'), None)


    //                             month_index.write('<a href="%s">Data file</a>  GeoTIFF %s<br/>\n' % (s3_base + product_file['data'], product_file['size']))
    //                             if clouds_file is not None:
    //                                 month_index.write('<a href="%s">Cloudmask</a> GeoTIFF %s<br/>\n' % (s3_base + clouds_file['data'], clouds_file['size']))
    //                             if sat_file is not None:
    //                                 month_index.write('<a href="%s">Saturated pixel mask</a> GeoTIFF %s<br/>\n' % (s3_base + sat_file['data'], sat_file['size']))
    //                             if valid_file is not None:
    //                                 month_index.write('<a href="%s">Valid pixel mask</a> GeoTIFF %s<br/>\n' % (s3_base + valid_file['data'], valid_file['size']))
    //                             if toposhad_file is not None:
    //                                 month_index.write('<a href="%s">Topographic shadow mask</a> GeoTIFF %s<br/>\n' % (s3_base + toposhad_file['data'], toposhad_file['size']))
    //                             if meta_file is not None:
    //                                 month_index.write('<a href="%s">Metadata</a> JSON %s\n' % (s3_base + meta_file['data'], meta_file['size']))
    //                             month_index.write('<hr/\n')
    //                             month_index.write('</td>\n')
    //                             month_index.write('</tr>\n')
            s.Append("</div></body></html>");


            string dir = Path.Combine(outputDir, year);
            Directory.CreateDirectory(dir);            
            File.WriteAllText(Path.Combine(dir, month + ".html"), s.ToString());
        }
    }
}