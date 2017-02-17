from netCDF4 import Dataset

class NetCDFMetadata:
    def getTimeCoverage(self, ncFile):
        ret = {}

        root = Dataset(ncFile, "r", format="NETCDF4")
        
        ret['start'] = root.time_coverage_start
        ret['end'] = root.time_coverage_end
        
        root.close()

        return ret
        