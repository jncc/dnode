
export class Product {
    id:                string // uuid
    title:             string // parsed from the filename
    footprint:         string // geojson
    properties: {
        capturedate:   string // date; parsed from the filename
        cloudcoverage: number // can't do right now
    }
    representations: {
        download: {
            url:       string // S3 URL i.e. https://s3-eu-west-1.amazonaws.com/eodip/ard/S2_20160719_37_4/S2_20160719_37_4.tif
            size:      number // file size in kilobytes
            type:      string // file type e.g. "Geotiff"
        },
        wms: {
            name:      string // potentially uuid from id
            base_url:  string // geoserver WMS URL i.e. https://eo.jncc.gov.uk/geoserver/ows
        }
    }
}
