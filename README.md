# traffic-example
An example application of traffic-engine.

# prereqs

* [osm-lib](https://github.com/conveyal/osm-lib)
* [traffic-engine](https://github.com/maptrace/traffic-engine)

# build

    $ mvn clean compile assembly:single

# usage

1. Grab a city extact, like https://s3.amazonaws.com/metro-extracts.mapzen.com/manila_phillipines.osm.pbf. Save it in $PROJECT_ROOT/data.
1. Get yourself a CSV containing GPS records in the form `time,vehicle_id,lon,lat`. For example: `2015-01-01 00:00:00.07018+08,43736,121.123456,14.123456`. Save it to $PROJECT_ROOT/data. You'll need about a million records before the results are any good. 
1. Import the project to Eclipse.
1. Edit the PBF_IN and CSV_IN fields in App.java to reflect your input PBF and CSV files.
1. Run App.java
1. Open the output shapefile (defined by the SHAPEFILE_OUT field) in QGIS.
1. Open the output speed CSV (defined by the CSV_OUT field) in QGIS. Specify that the CSV has no geometry.
1. Open the properties of the shapefile in QGIS. Click the 'Joins' tab. 
1. Click the little green 'plus' button. The join field is 'waysegid' and the target field is 'name'.
1. Click on the 'Style' tab. Select the 'Spectral' color ramp for the column "manila_speeds_mean". Use the 'Quantile' mode, with enough classes to show an even gradient. 30 works fine.
1. Click 'OK'. The resulting image should be pretty.
