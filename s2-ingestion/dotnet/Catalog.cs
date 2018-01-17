using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace dotnet
{
    public static class Catalog
    {
        static string outputDir = "../output/catalog";
        
        public static void GenerateJson(List<Product> products)
        {
            Directory.CreateDirectory(outputDir);
            Console.WriteLine("Generating catalog json...");

            // todo make the right output shape
            var output = products;

            products.Select(MakeProductJson);

            string json = JsonConvert.SerializeObject(output, Formatting.Indented);
            File.WriteAllText(Path.Combine(outputDir, "catalog.json"), json);
        }


        static dynamic MakeProductJson(Product p)
        {
            return new {
                name = p.Name,
                collectionName = "eocoe/sentinel/2/processed/ard/default",
                metadata = new {
                    title = "Sentinel 2 ARD " + p.Name,
                    keywords = new [] { new { value = "Sentinel-2" }, new { value = "ARD" }  },
                    lineage = "***************TODO***************",
                    @abstract = "***************TODO***************",
                    metadataDate = "2018-01-15",
                    resourceType = "Dataset",
                    topicCategory = "imageryBaseMapsEarthCover", // think this is the correct INSPIRE / Gemini category
                    useConstraints = "***************TODO***************",
                    metadataLanguage = "English",
                    accessLimitations = "no limitations", // this field should be called limitationsOnPublicAccess
                    datasetReferenceDate = "2018-01-15", // or change to publication date
                    metadataPointOfContact = new {
                        name = "***************TODO***************",
                        email = "***************TODO***************",
                        role = "metadataPointOfContact"
                    },
                    spatialReferenceSystem = "***************TODO***************", // what's the EPSG code for UTM/military grid?
                        responsibleOrganisation = new {
                            name  = "Scottish Government",
                            email = "ceu@gov.scot",
                        },
                    //     "resourceLocator": "catalog://04-05scotland-gov-gi/lidar-1/processed/dtm/gridded/27700/10000/NH87",
                    //     "temporalExtent": {
                    //       "begin": "2011-03-01T00:00:00Z",
                    //       "end": "2012-05-01T00:00:00Z"
                    //     },
                    //     "limitationsOnPublicAccess": "None",
                    //     "boundingBox": {
                    //       "north": 57.7960680443262,
                    //       "south": 57.7062934711795,
                    //       "east": -3.85220733512446,
                    //       "west": -4.0203233299185

                }
            };
        }
    }
}