create table sample_details
(
	user_id int primary key,
	created_at timestamp,
	last_created_at timestamp,
	status	 varchar(64),
	is_black_listed bool,
	initiated timestamp,
	pending_verification timestamp,
	verified timestamp,
	modified_at timestamp not null default now()
);

create index idx_sample_details_user_id on sample_details(user_id);

commit;

create table staging_sample_details
(
	user_id int primary key,
	created_at timestamp,
	last_created_at timestamp,
	status	 varchar(64),
	is_black_listed bool,
	initiated timestamp,
	pending_verification timestamp,
	verified timestamp,
	modified_at timestamp not null default now()
);

create index idx_staging_sample_details_user_id on staging_sample_details(user_id);

commit;

select * from sample_details;
select * from staging_sample_details;
