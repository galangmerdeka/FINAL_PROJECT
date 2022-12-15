drop table if exists dim_city;

create table dim_city (
	id varchar(255) NULL,
	state_id varchar(255) null,
	city_name varchar(255) null,
	zipcode varchar(10) null
);

insert into dim_city(
	id,
	state_id,
	city_name,
	zipcode
) (
	select 
	gen_random_uuid() as id,
	ds.id as state_id, 
	z.city as city_name, 
	z.zip as zipcode
	from companies c 
	 join zips z on c.offices_zipcode = z.zip 
	 join dim_state ds on z.state = ds.state_code 
	 group by ds.id, z.city, z.zip
);
