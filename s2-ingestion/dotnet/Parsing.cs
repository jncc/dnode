
namespace dotnet
{
    public static class Parsing
    {
        // change from python version: remove unnecessary underscores which aren't accepted by .net regex engine 
        public static string ExtractionRegex = @"Sentinel2([AB])_((20[0-9]{2})([0-9]{2})([0-9]{2}))\/SEN2_[0-9]{8}_lat([0-9]{2,4})lon([0-9]{2,4})_T([0-9]{2}[A-Z]{3})_ORB([0-9]{3})_(utm[0-9]{2}n)(_osgb)?_(clouds|sat|toposhad|valid|vmsk_sharp_rad_srefdem_stdsref|meta|thumbnail)(?!\.tif\.aux\.xml)";
            
    }
}