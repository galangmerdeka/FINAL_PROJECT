drop table if exists dim_state;

create table dim_state (
	id varchar(255) NULL,
	country_id varchar(255) null,
	state_code varchar(10) NULL
);

insert into dim_state(
	id,
	country_id,
	state_code 
) (
	select gen_random_uuid() as id, 
	dc.id as country_id, 
	c.offices_state_code as state_code 
	from dim_country dc 
	left join companies c on dc.country_code = c.offices_country_code 
	where c.offices_state_code is not null
);