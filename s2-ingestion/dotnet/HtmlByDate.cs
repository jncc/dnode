
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace dotnet
{
    public static class HtmlByDate
    {
        public static void GenerateHtml(IEnumerable<IGrouping<string, Asset>> products)
        {
            Console.WriteLine("Generating HTML by date...");

            // with open(os.path.join(outdir, 'index.html'), 'w') as index:
        
            var s = new StringBuilder();

    //     <style>
    //         td {
    //             border-bottom: 1px solid #bfc1c3;
    //             padding-bottom: 0.05em;
    //             text-align: center;
    //         }
    //     </style>        

            s.Append(@"<html>
                        <head>
                        <title>Sentinel 2 ARD Index</title>
                        <link rel=""stylesheet"" href=""https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css""/>
                        </head>
                        <body>
                        <div class=""ui container"">
                        <h1>Sentinel-2 ARD Index</h1>
                        <table width=80%>
                        <thead>
                        <td>Year</td>
                        <td>Month</td>
                        </thead>
                        <tbody>");
            // var years = from p in products
            //             group p by p.Key.y
        
    //     for year in data:
    //         index.write('<tr>\n')
    //         index.write('<td width=25%%>%s</td>\n' % (year))
    //         index.write('<td>\n')
    //         for month in data[year]:
    //             index.write('<a href="%s/%s.html">%s</a><br/>\n' % (year, month, calendar.month_name[int(month)]))
    //             year_dir_path = os.path.join('.', outdir, year)
    //             if not os.path.exists(year_dir_path):
    //                 os.makedirs(year_dir_path)
    //             with open(os.path.join(year_dir_path, '%s.html' % month), 'w') as month_index:
    //                 month_index.write('<html><head><title></title><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.2.13/semantic.min.css"/></head><body><div class="ui container">\n')
    //                 month_index.write('<h1>%s %s</h1>' % (year, calendar.month_name[int(month)]))
    //                 month_index.write('<table>\n')
    //                 for day in data[year][month]:
    //                     for grid in data[year][month][day]:
    //                         s3_base = 'https://s3-eu-west-1.amazonaws.com/eocoe-sentinel-2/'
    //                         p = data[year][month][day][grid] # the product at this gridsquare
    //                         product_file = next((f for f in p['files'] if f['type']=='product'), None)
    //                         clouds_file = next((f for f in p['files'] if f['type']=='clouds'), None)
    //                         meta_file = next((f for f in p['files'] if f['type']=='meta'), None)
    //                         sat_file = next((f for f in p['files'] if f['type']=='sat'), None)
    //                         toposhad_file = next((f for f in p['files'] if f['type']=='toposhad'), None)
    //                         valid_file = next((f for f in p['files'] if f['type']=='valid'), None)
    //                         if product_file is not None:
    //                             thumbnail_file = 'thumbnails/' + os.path.basename(product_file['data'].replace('_vmsk_sharp_rad_srefdem_stdsref.tif', '_thumbnail.jpg'))
    //                             month_index.write('<tr>\n')
    //                             month_index.write('<td><img src="%s" height=100px width=100px></td>\n' % (s3_base + thumbnail_file))
    //                             month_index.write('<td>\n')
    //                             month_index.write('<h3>%s</h3>\n' % (p['name']))
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
    //                 month_index.write('</table>\n')
    //                 month_index.write('</div></body></html>')
    //         index.write('</td>\n')
    //         index.write('</tr>\n')

            s.Append("</tbody></table></div></body></html>");

            Console.WriteLine(s);
        }
    }
}