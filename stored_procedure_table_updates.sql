create or replace procedure update_staging_sample_details()

language plpgsql

as $$

begin 
	with latest_data as 
	(
		select
		user_id,
		max(last_created_at) as max_last_created_at
		from 
		sample_details
		group by 1
	)
	insert into staging_sample_details 
	(
		user_id,
		created_at,
		last_created_at,
		status,
		is_black_listed,
		initiated,
		pending_verification,
		verified
	)
	select
		sd.user_id,
		sd.created_at,
		sd.last_created_at,
		sd.status,
		sd.is_black_listed,
		sd.initiated,
		sd.pending_verification,
		sd.verified
	from sample_details sd
	inner join latest_data ld on sd.user_id = ld.user_id 
					and sd.last_created_at=ld.max_last_created_at
	on conflict(user_id) do update 
	set
		user_id = excluded.user_id,
		created_at = excluded.created_at ,
		last_created_at = excluded.last_created_at,
		status = excluded.status,
		is_black_listed = excluded.is_black_listed,
		initiated = excluded.initiated,
		pending_verification = excluded.pending_verification,
		verified = excluded.verified;
end;
$$ 

call update_staging_sample_details();
