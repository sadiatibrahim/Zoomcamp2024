-- Question 1

CREATE MATERIALIZED VIEW aggregate_view AS
select AVG(trip_time) as average_time,
	   MIN(trip_time) as minimum_time,
	   MAX(trip_time) as maximum_time,
	   pickup_zone,
	   dropoff_zone
	   
FROM 
	(
	 select EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime))/60 as trip_time, pickup_zone.zone as pickup_zone, dropoff_zone.zone as dropoff_zone
	 from trip_data td inner join taxi_zone pickup_zone 
	 on td.PUlocationid = pickup_zone.location_id 
	 inner join taxi_zone dropoff_zone on td.Dolocationid = dropoff_zone.location_id
	 
	)
	
group by pickup_zone, dropoff_zone;
	 
	 
	 
select pickup_zone, dropoff_zone 
from aggregate_view
order by average_time desc
limit 1;


-- Question 2


CREATE MATERIALIZED VIEW aggregate_view1 AS
with cte as (
	select AVG(trip_time) as average_time,
		   MIN(trip_time) as minimum_time,
		   MAX(trip_time) as maximum_time,
		   pickup_zone,
		   dropoff_zone
		   
	FROM 
		(
		 select EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime))/60 as trip_time, pickup_zone.zone as pickup_zone, dropoff_zone.zone as dropoff_zone
		 from trip_data td inner join taxi_zone pickup_zone 
		 on td.PUlocationid = pickup_zone.location_id 
		 inner join taxi_zone dropoff_zone on td.Dolocationid = dropoff_zone.location_id
		 
		)
		
	group by pickup_zone, dropoff_zone
),

cte2 as (
	select pickup_zone.zone as pickup_zone, dropoff_zone.zone as dropoff_zone
		 from trip_data td inner join taxi_zone pickup_zone 
		 on td.PUlocationid = pickup_zone.location_id 
		 inner join taxi_zone dropoff_zone on td.Dolocationid = dropoff_zone.location_id
		 
		)
select count(*) as number_trips
from cte2
where pickup_zone = (select pickup_zone from cte order by average_time desc limit 1)
and dropoff_zone = (select dropoff_zone from cte order by
average_time desc limit 1);


-- Question3
CREATE  MATERIALIZED VIEW busiest_zone_view as
with cte as (
		select distinct pickup_zone.zone as pickup_zone, tpep_pickup_datetime
		from trip_data td inner join taxi_zone pickup_zone 
		on td.PUlocationid = pickup_zone.location_id 
		 
	),
	cte2 as (select maximum - INTERVAL '17 hours' as timestamp_before
				FROM (select max(tpep_pickup_datetime) as maximum from cte)
		)
	select  pickup_zone, tpep_pickup_datetime
	from cte
	where tpep_pickup_datetime BETWEEN(select timestamp_before from cte2 limit 1) AND (select  max(tpep_pickup_datetime) from cte limit 1);


select distinct pickup_zone,  count(*) as count from busiest_zone_view
group by pickup_zone
order by count desc
limit 3;
	