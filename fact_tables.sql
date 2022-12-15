drop table if exists fact_table;

create table fact_table (
	state varchar(255) NULL,
	total_city int null,
	total_offices int null
);

insert into fact_table(
	state,
	total_city,
	total_offices
) (
	select ds.state_code, COUNT(dc.city_name) as total_city, COUNT(dc2.country_code) as total_offices from dim_city dc 
	left join dim_state ds on dc.state_id = ds.id 
	join dim_country dc2 on ds.country_id = dc2.id 
	join companies c on dc2.country_code = c.offices_country_code 
	group by ds.state_code 
);
