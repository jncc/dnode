
#! /bin/sh
# ==================================================================
#  ______                           __     _____
# /_  __/___  ____ ___  _________ _/ /_   /__  /
#  / / / __ \/ __ `__ \/ ___/ __ `/ __/     / /
# / / / /_/ / / / / / / /__/ /_/ / /_      / /
#/_/  \____/_/ /_/ /_/\___/\__,_/\__/     /_/

# Multi-instance Apache Tomcat installation with a focus
# on best-practices as defined by Apache, SpringSource, and MuleSoft
# and enterprise use with large-scale deployments.

# Credits:
#       Google -> Couldn't survive without it
#       Stackoverflow.com -> Community support
#       SpringSource -> Specifically best-practices and seminars (Expert Series)

# Based On:
#       http://www.springsource.com/files/uploads/tomcat/tomcatx-performance-tuning.pdf
#       http://www.springsource.com/files/u1/PerformanceTuningApacheTomcat-Part2.pdf
#       http://www.springsource.com/files/uploads/tomcat/tomcatx-large-scale-deployments.pdf

# Created By: Terrance A. Snyder
# URL: http://www.terranceasnyder.com, http://shutupandcode.net

# Best Practice Documentation:
# http://terranceasnyder.com/2011/05/tomcat-best-practices/

# Looking for the latest version?
# github @ https://github.com/terrancesnyder

# ==================================================================

# # discourage address map swapping by setting Xms and Xmx to the same value
# # http://confluence.atlassian.com/display/DOC/Garbage+Collector+Performance+Issues
# export CATALINA_OPTS="$CATALINA_OPTS -Xms64m"
# export CATALINA_OPTS="$CATALINA_OPTS -Xmx512m"

# # Increase maximum perm size for web base applications to 4x the default amount
# # http://wiki.apache.org/tomcat/FAQ/Memoryhttp://wiki.apache.org/tomcat/FAQ/Memory
# export CATALINA_OPTS="$CATALINA_OPTS -XX:MaxPermSize=256m"

# # Reset the default stack size for threads to a lower value (by 1/10th original)
# # By default this can be anywhere between 512k -> 1024k depending on x32 or x64
# # bit Java version.
# # http://www.springsource.com/files/uploads/tomcat/tomcatx-large-scale-deployments.pdf
# # http://www.oracle.com/technetwork/java/hotspotfaq-138619.html
# export CATALINA_OPTS="$CATALINA_OPTS -Xss192k"

# # Oracle Java as default, uses the serial garbage collector on the
# # Full Tenured heap. The Young space is collected in parallel, but the
# # Tenured is not. This means that at a time of load if a full collection
# # event occurs, since the event is a 'stop-the-world' serial event then
# # all application threads other than the garbage collector thread are
# # taken off the CPU. This can have severe consequences if requests continue
# # to accrue during these 'outage' periods. (specifically webservices, webapps)
# # [Also enables adaptive sizing automatically]
# export CATALINA_OPTS="$CATALINA_OPTS -XX:+UseParallelGC"

# # This is interpreted as a hint to the garbage collector that pause times
# # of <nnn> milliseconds or less are desired. The garbage collector will
# # adjust the  Java heap size and other garbage collection related parameters
# # in an attempt to keep garbage collection pauses shorter than <nnn> milliseconds.
# # http://java.sun.com/docs/hotspot/gc5.0/ergo5.html
# export CATALINA_OPTS="$CATALINA_OPTS -XX:MaxGCPauseMillis=1500"

# # A hint to the virtual machine that it.s desirable that not more than:
# # 1 / (1 + GCTimeRation) of the application execution time be spent in
# # the garbage collector.
# # http://themindstorms.wordpress.com/2009/01/21/advanced-jvm-tuning-for-low-pause/
# export CATALINA_OPTS="$CATALINA_OPTS -XX:GCTimeRatio=9"

# The hotspot server JVM has specific code-path optimizations
# # which yield an approximate 10% gain over the client version.
# export CATALINA_OPTS="$CATALINA_OPTS -server"

# Geoserver Opts
# When ingesting and serving time-series data, GeoServer needs to be run in a web container that has the timezone properly configured in order to avoid problems during requests due to wrong timezone applied when parsing and/or ingesting timestamp values. To set the time zone to be Coordinated Universal Time (UTC) (which we strongly recommend), you should add the following switch when launching the Java process for GeoServer. This will ensure that every time a String representing a time is parse or encoded it is done using the GMT timezone; the same applies to when Date objects area created internally in Java.

export JAVA_OPTS="$JAVA_OPTS -Duser.timezone=GMT"

# If you are using a shapefile as the mosaic index store, another java process option is needed to enable support for timestamps in shapefile stores:
# export JAVA_OPTS="$JAVA_OPTS -Dorg.geotools.shapefile.datetime=true"

# When accessing NetCDF datasets, some auxiliary files are automatically created. By default they will be created beside the original data. However, in order to avoid write permission issues with the directories containing the NetCDF datasets, is it possible to configure an external directory to contain the auxiliary files. Add this switch to the java process

# export JAVA_OPTS="$JAVA_OPTS -DNETCDF_DATA_DIR=%DATA_DIR%/netcdf_data_dir"

# When accessing GRIB datasets, some auxiliary files are automatically created and cached to speedup the access. By default they will be created beside the original data. However, in order to avoid write permission issues with the directories containing the GRIB datasets, is it possible to configure an external directory to contain the cache files. Add this switch to the java process

# export JAVA_OPTS="$JAVA_OPTS -DGRIB_CACHE_DIR=%TRAINING_ROOT%/grib_cache_dir"