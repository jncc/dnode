
export class Product {
    id:                string // uuid
    title:             string // parsed from the filename
    footprint:         string // geojson
    properties: {
        endposition:   string // date; parsed from the filename
        cloudcoverage: number // can't do right now
    }
    representations: {
        download: {
            url:       string // S3 URL
            size:      number // file size in kilobytes
            type:      string // file type e.g. "Geotiff"
        },
        wms: {
            name:      string // potentially uuid from id
            base_url:  string // geoserver WMS URL i.e. https://eo.jncc.gov.uk/geoserver/ows
        }
    }
}
