CREATE TEMPORARY FUNCTION geohash_decode(geohash STRING)
 RETURNS STRUCT<lat FLOAT64,lon FLOAT64>
  LANGUAGE js AS "return Geohash.decode(geohash);"
 OPTIONS (
  library="gs://bi-datafiles/UDFs/latlon-geohash.js"  
);

CREATE TEMPORARY FUNCTION geohash_neighbours_array(geohash STRING)
RETURNS ARRAY<string>
 LANGUAGE js AS "return Geohash.neighbours_array(geohash);"
OPTIONS (
 library="gs://bi-datafiles/UDFs/bi_geohash.js"  
);

CREATE TEMPORARY FUNCTION geohash_encode(lat FLOAT64, lon FLOAT64, precision INT64)
 RETURNS STRING
  LANGUAGE js AS "return geotools.encode(lat, lon, precision);"
 OPTIONS (
  library="gs://geotab-udfs/geotools.js"  
);


With event AS
(
 --this pulls status data from the previous month where the status code is 77, and then finds where the status code has increased more than 70% (indicating a renewal of engine oil life)
    Select SerialNo, LastValue, CodeDate, DateTime, CodeGeohash AS CenterGeohash 
      From
         (
        Select SerialNo, LastValue, Date(DateTime) As CodeDate, DateTime, SUBSTR(Geohash, 1, 8) AS CodeGeohash
          From
             (
            Select SerialNo, DateTime, Code, Value, Geohash, Speed, 
                   LAG(Value, 1) OVER (PARTITION BY SerialNo ORDER BY SerialNo, DateTime) AS LastValue
             From `geotab-dsp-$name.Interpolated.Status`
            Where _PARTITIONTIME between timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01')) 
                  and timestamp_sub(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01')), interval 1 DAY)
                  and DATE(DateTime) between Date(timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01'))) 
                  and date_sub(Date(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01'))), interval 1 DAY)
                  and Code = 77 and Value != 0)
        Where Value-LastValue >= 70 and Speed = 0
    )) 
   
 --this query will allow you to find the location of when the Engine Oil Life Remaining changed.
, servicetime AS
(
  Select A.SerialNo, LastValue, StartTime, StopTime, CodeDate, CenterGeohash, 
         avg(StopLatitude) as AvgLat, avg(StopLongitude) as AvgLon,
         StopDuration
   From
      (
    Select SerialNo, LastValue, CodeDate, DateTime, CenterGeohash, DateTime,
           CONCAT(geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(1)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(2)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(3)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(4)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(5)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(6)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(7)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(8)], ',' ,
                  geohash_neighbours_array(SUBSTR(CenterGeohash,1,8))[ORDINAL(9)]) AS CenterGeohashNbrs
     From event
  ) A
   Inner Join
 --this subquery pulls in the vehicle's location, using the trips table.
  (
    Select SerialNo, Min(LagStop) as StartTime, Max(LeadStart) as StopTime, Timestamp_Diff(Max(LeadStart),Min(LagStop), minute) as StopDuration,
           Avg(StopLatitude) As StopLatitude, Avg(StopLongitude) As StopLongitude, geohash_encode(Avg(StopLatitude), Avg(StopLongitude), 7) As StopGeohash, Sum(Distance) As Distance, Cluster 
     From
       (
       Select *, Countif(distance>1 or lagdist>1) over (Partition by Serialno Order by StopTime) Cluster 
         From 
            (
             Select *, LAG(Distance) OVer (PArtition by SerialNo ORder by StopTime) AS LagDist, 
                    LEAD(StartTime) OVER (Partition By SerialNo Order by StartTime) As LeadStart, Lag(StopTime) Over (PArtition by SerialNo Order by StopTime) as LagStop 
              From
                 (
                 Select SerialNo, Distance, StartTime, StopTime, StopDuration, StopLatitude, StopLongitude,
                        geohash_encode(StopLatitude, StopLongitude, 8) AS StopGeohash
                  From `geotab-dsp-$name.Interpolated.Trips`
                 Where _PartitionTime between timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01')) 
                       and Timestamp_sub(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01')), interval 1 DAY)
                       and Date(StopTime) between Date(timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01'))) 
                       and Date_sub(Date(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01'))), interval 1 DAY))))
        Group by Serialno, Cluster
    ) B
       On A.SerialNo = B.SerialNo AND REGEXP_CONTAINS(CenterGeohashNbrs, StopGeohash) = TRUE 
          and Datetime between StartTime and StopTime
    Group by SerialNo, LastValue, CodeDate, CenterGeohash, StopDuration, StartTime, StopTime
)

 --once we have the date and time, we can connect the vehicle with its VIN and the make, model, and year or vehicle type. 
, cleanevent AS
(
  Select SerialNo, LastValue
         Avg(AvgLat) as AvgLat, Avg(AvgLon) as AvgLon, VehicleType, WeightClass,
        CodeDate as DateofRepair, CycleHour
   From
  (
    Select *
      From
    (
      Select A.SerialNo, CodeDate,
             Avg(AvgLat) as AvgLat, Avg(AvgLon) as AvgLon, 
             SUM(StopDuration)/3600 AS CycleHour, Vin
      From servicetime A
     Inner Join `geotab-dsp-$name.Interpolated.Vin B
       On A.SerialNo = B.SerialNo and (CodeDate >= Date(DateFrom)) 
         and ((CodeDate <= Date(DateTo)) or (DateTo is Null))
     Group By SerialNo, CodeDate, CenterGeohash, Vin
    ) C
    Left Join
    (
      Select VehicleType, WeightClass, Vin
        From `geotab-dsp-$name.Vin.VinDecode`
    ) D
     On C.Vin = D.Vin
  )
  Group By SerialNo, LastValue, CenterGeohash, VehicleType, 
           WeightClass, CycleHour, CodeDate
)

Select * From cleanevent