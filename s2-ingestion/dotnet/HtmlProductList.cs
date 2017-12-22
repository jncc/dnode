using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace dotnet
{
    public class HtmlProductList
    {
        static string s3BasePath = "https://s3-eu-west-1.amazonaws.com/eocoe-sentinel-2/";
        
        public static void Generate(StringBuilder s, IEnumerable<Product> products)
        {

            s.Append("<div class=\"ui items\">");

            foreach (var p in products)
            {
                string monthName = CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(int.Parse(p.Attrs.month));
                
                    var dataFile = p.Files.Single(f => f.type == "data");
                    string thumbnailPath = "thumbnails/" + Path.GetFileName(dataFile.path.Replace("_vmsk_sharp_rad_srefdem_stdsref.tif", "_thumbnail.jpg"));

                    Action<string, string, string> renderFile = (name, type, filetype) =>
                    {
                        var file = p.Files.SingleOrDefault(f => f.type == type);
                        if (file == null)
                            s.Append($"<div>No {name} available</div>");
                        else
                            s.Append($"<div><a href=\"{s3BasePath + file.path}\">{name}</a> {filetype} {file.size}</div>");
                    };

                    s.Append("<div class=\"item\">");
                    s.Append("<div class=\"image\">");
                    s.Append($"<img src=\"{s3BasePath + thumbnailPath}\" height=\"100px\" width=\"100px\">");
                    s.Append("</div>");
                    s.Append("<div class=\"content\">");
                    s.Append($"<div class=\"header\">{p.Name}</div>");
                    s.Append("<div class=\"meta\">");
                    s.Append($"<span>{p.Attrs.day} {monthName} {p.Attrs.day}</span>");
                    s.Append("</div>");
                    s.Append("<div class=\"extra\">");
                    renderFile("Data file", "data", "Geotiff");
                    renderFile("Cloud file", "clouds", "Geotiff");
                    renderFile("Saturated pixel mask", "sat", "Geotiff");
                    renderFile("Valid pixel mask", "valid", "Geotiff");
                    renderFile("Topographic shadow mask", "toposhad", "Geotiff");
                    renderFile("Metadata", "meta", "Json");
                    s.Append("</div>"); // extra
                    s.Append("</div>"); // content
                    s.Append("</div>"); // item
            }

            s.Append("</div>"); // ui items
        }
    }
}

