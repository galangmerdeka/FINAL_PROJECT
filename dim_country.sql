drop table if exists dim_country;

create table dim_country (
	id varchar(255) NULL,
	country_code varchar(10) NULL
);

insert into dim_country(
	id,
	country_code 
) (
	select
		gen_random_uuid() as id,
		c.offices_country_code  as country_code 
		from 
		companies c
		group by c.offices_country_code 
);
