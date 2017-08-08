# Build the image
    docker build -t geoserver .
# Run interactivly
    docker run -it geoserver  /bin/bash
# Just run it
    -p 8888:8080