

create database movies;

create or replace table raw_movie_bookings(
    booking_id string,
    customer_id string,
    movie_id string,
    booking_date timestamp,
    status string,
    ticket_count int,
    ticket_price number(10,2));


create or replace stream movie_bookings_stream
on table raw_movie_bookings;

select * from movie_bookings_stream;

create or replace table movie_bookings_cdc_events(
    booking_id string,
    customer_id string,
    movie_id string,
    booking_date timestamp,
    status string,
    ticket_count int,
    ticket_price number(10,2),
    change_type string,
    is_update boolean,
    change_timestamp timestamp);

--Bronze Layer
create or replace task ingest_cdc_events_task
warehouse = 'COMPUTE_WH'
schedule = '1 Minute'
as 
insert into movie_bookings_cdc_events
    select 
        booking_id,
        customer_id,
        movie_id,
        booking_date,
        status,
        ticket_count,
        ticket_price,
        METADATA$ACTION as change_type,
        METADATA$ISUPDATE as is_update,
        current_timestamp() AS change_timestamp
    from movie_bookings_stream;

show tasks;

ALTER TASK ingest_cdc_events_task SUSPEND;


--Silver Layer
create or replace dynamic table movie_bookings_filtered
warehouse = 'COMPUTE_WH'
target_lag = DOWNSTREAM
as 
    select 
        booking_id,
        customer_id,
        movie_id,
        booking_date,
        status,
        ticket_count,
        ticket_price,
        max(change_timestamp) as latest_timestamp
    from movie_bookings_cdc_events
    where change_type IN ('INSERT','DELETE')
    group by booking_id,customer_id,movie_id,booking_date,status,ticket_count,ticket_price;

--Gold Layer

create or replace dynamic table movie_bookings_insights
warehouse = 'COMPUTE_WH'
target_lag = DOWNSTREAM
as
    select 
        movie_id,
        count(booking_id) as total_bookings,
        sum(case when status = 'COMPLETED' then ticket_count else 0 end) as total_tickets_sold,
        sum(case when status = 'COMPLETED' then ticket_price else 0 end) as total_revenue,
        count(case when status = 'CANCELLED' then 1 else 0 end) as total_cancellations,
        current_timestamp() as refresh_timestamp
    from movie_bookings_filtered
    group by movie_id;


    create or replace task refresh_movie_bookings_insights
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '2 MINUTE'
    as 
    alter dynamic table movie_bookings_insights REFRESH;

alter task refresh_movie_bookings_insights SUSPEND;

show tasks;

insert into raw_movie_bookings values 
    ('B001','C001','M001','2025-12-29 10:00:00','BOOKED',2,15.00),
    ('B002','C002','M002','2025-12-29 10:10:00','BOOKED',1,12.00),
    ('B003','C003','M003','2025-12-29 10:15:00','BOOKED',3,20.00),
    ('B004','C004','M004','2025-12-29 10:20:00','BOOKED',4,25.00),
    ('B005','C005','M005','2025-12-29 10:25:00','BOOKED',5,30.00)


SELECT * FROM raw_movie_bookings;

SELECT * FROM movie_bookings_stream;

SELECT * FROM movie_bookings_cdc_events;

SELECT * FROM movie_bookings_filtered;

SELECT * FROM movie_bookings_insights;
        

select * from table(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'ingest_cdc_events_task')) order by SCHEDULED_TIME;

update raw_movie_bookings
set status = 'COMPLETED'
where booking_id in ('B001','B003');