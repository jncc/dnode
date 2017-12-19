using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace dotnet
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var lines = File.ReadLines(@"./../saved/list-20171206-101946.txt");

            Console.WriteLine("There are {0} S3 keys in the input file.", lines.Count());

            var q = from line in lines
                    let tokens = line.Split(' ')
                    let key = tokens[0].ToString()
                    let size = tokens[1].ToString()
                    let match = Regex.Match(key, Parsing.ExtractionRegex)
                    where match.Success
                    let product = Parse(key, size, match)
                    select keys;

            

        }
    
        static object Parse(string key, string size, Match match)
        {
            return new Product
            {
                s3_key=              key,
                s3_size=             size,
                satellite=           match.Groups[1],
                full_date=           match.Groups[2],
                year=                match.Groups[3],
                month=               match.Groups[4],
                day=                 match.Groups[5],
                lat=                 match.Groups[6],
                lon=                 match.Groups[7],
                grid=                match.Groups[8],
                orbit=               match.Groups[9],
                original_projection= match.Groups[10],
                new_projection=      match.Groups[11], // Optional Group, could give None
                file_type=           match.Groups[12],
            };
        }
    }

    class Product
    {
        string s3_key;
        string s3_size;
        string satellite;
        string full_date;
        string year;
        string month;
        string day;
        string lat;
        string lon;
        string grid;
        string orbit;
        string original_projection;
        string new_projection;
        string file_type;
    }
}
