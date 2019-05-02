With event AS
(
 --this pulls status data from the previous month where the status code is 77, and then finds where the status code has increased more than 70% (indicating a renewal of engine oil life)
    Select SerialNo, LastValue, CodeDate, DateTime, G as LocPoint
      From
         (
        Select SerialNo, LastValue, Date(DateTime) As CodeDate, DateTime, ST_Geogpoint(Longitude, Latitude) As G
          From
             (
            Select Serialno, DateTime, Code, Value, Longitude, Latitude, Speed, 
                   LAG(Value, 1) OVER (PARTITION BY HardwareId ORDER BY HardwareId, DateTime) AS LastValue
             From `geotab-dsp-$name.Interpolated.Status`
            Where _PARTITIONTIME between timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01')) 
                  and timestamp_sub(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01')), interval 1 DAY)
                  and DATE(DateTime) between Date(timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01'))) 
                  and date_sub(Date(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01'))), interval 1 DAY)
                  and Code = 77 and Value != 0
                  And Geohash not like 's00%')
        Where Value-LastValue >= 70 and Speed = 0
          
    )) 
    

 --this query will allow you to find the location of when the Engine Oil Life Remaining changed.
, servicetime AS
(
  Select A.SerialNo, LastValue, StartTime, StopTime, CodeDate,
         StopDuration 
   From
      (
    Select SerialNo, LastValue, CodeDate, DateTime, ST_Centroid(LocPoint) AS Location
     From event
  ) A
   Inner Join
 --this subquery pulls in the vehicle's location, using the trips table.
  (
    Select SerialNo, Min(Stoptime) as StartTime, Max(LeadStart) as StopTime, Timestamp_Diff(Max(LeadStart),Min(StopTime), minute) as StopDuration,
           St_Geogpoint(Avg(StopLongitude), Avg(StopLatitude)) As G, Sum(Distance) As Distance, Cluster 
     From
       (
       Select *, Countif(distance>1) over (Partition by Serialno Order by StopTime) Cluster 
         From 
            (
             Select *, LAG(Distance) OVer (PArtition by SerialNo Order by StopTime) AS LagDist, 
                    LEAD(StartTime) OVER (Partition By SerialNo Order by StartTime) As LeadStart
              From
                 (
                 Select Serialno, Distance, StartTime, StopTime, StopDuration, StopLatitude, StopLongitude,
                        StopGeohash
                  From `geotab-dsp-$name.Interpolated.Trips`
                  Where StartTime between timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01')) 
                       and Timestamp_sub(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01')), interval 1 DAY)
                       and Date(StopTime) between Date(timestamp(concat(substr(string(timestamp(Date_sub(current_Date(), interval 1 MONTH))), 1, 8),'01'))) 
                       and Date_sub(Date(timestamp(concat(substr(string(current_timestamp()), 1, 8),'01'))), interval 1 DAY)
                       And StartGeohash not like 's00%' And StopGeohash not like 's00%'
                       )))
                       
        Group by Serialno, Cluster
    ) B
       On A.SerialNo = B.SerialNo AND ST_Distance(Location, G) > 500
      and Datetime between StartTime and StopTime
    Group by SerialNo, LastValue, CodeDate, StopDuration, StartTime, StopTime
)

Select * from(
  Select S.*, e.Datetime 
    From ServiceTime S Inner Join Event E 
      on S.Serialno=E.Serialno
   Where DAtetime between Starttime and StopTime)
