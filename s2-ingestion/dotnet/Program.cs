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
                    select key;

            

        }
    
        static object Parse(string key, string size, Match match)
        {

        }
    }

    class Product
    {

    }
}
