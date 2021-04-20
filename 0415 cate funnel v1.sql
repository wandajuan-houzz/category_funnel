-- Product category funnel for week starting on Mar14 2021, Mar21 2021, Mar28 2021 and April 4 2021 (starting on Sunday)
-- for each top category (outdoor products, furniture, bath products, lighting, hom decor and others)
-- #Page_views (#VIEW_PRODUCT + #pvp)
-- #Add_to_Carts
-- #Checkouts






-- Product category
select * from shop.vl_pupil;
select vendor_listing_id, house_id, category_name, l1_category_name from shop.vl_pupil;

-- #Page_views for each item of category - via web
with t as (

	select 
			if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('home improvement', 'furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
		    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as prod_view_week, 
			*,
			vl.house_id as vlhouse_id
	from (
		select 
		page_behavior,
		if(page_id is null, cast(regexp_extract(url, '(.*pv~)(\d+)(.*)', 2) as bigint), page_id) as house_id,
--		page_id, url, topic, 
		'web' as source,
		dt 
		from l2.page_views_daily 
		where page_behavior in ('VIEW_PRODUCT', 'pvp')
		and cast(dt as date) >= date '2021-03-14'
		
		union all 
		
		select 
			null as page_behavior,
			cast(object_id as bigint) as house_id, 
--			session_id,
--			device_id,
			'app' as source,
			dt
		from l2.mobile_client_event -- app sess 
		where event_type = 'View'
		and object_id is not null and entity_type = 'Product'
		and (context <> 'Back' or context is null)
		and dt >= '2021-03-14'
		
		) i 
	left join shop.vl_pupil vl
	on i.house_id = vl.house_id
)
select category, source, page_behavior, prod_view_week, count(*) as num_prod_views from t
group by 1, 2, 3, 4
;


-- what are those `N/A`?

-- 154K unique house_id without category
select * from (
	select 
			if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('home improvement', 'furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
		    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as prod_view_week, 
			i.*,
			vl.house_id as vlhouse_id
	from (
		select 
		page_behavior,
		if(page_id is null, cast(regexp_extract(url, '(.*pv~)(\d+)(.*)', 2) as bigint), page_id) as house_id,
		page_id, url, topic, dt from l2.page_views_daily 
		where page_behavior in ('VIEW_PRODUCT', 'pvp')
		and cast(dt as date) >= date '2021-03-14'
		) i 
	left join shop.vl_pupil vl
	on i.house_id = vl.house_id
)
where category = 'N/A';

https://www.houzz.co.uk/products/outdoor-concrete-dining-table-small-prvw-vr~121325557
https://www.houzz.com/products/versailles-bookcase-prvw-vr~130954
https://www.houzz.com.au/products/armchairs-prvw-vr~43359461


-- #Page_views for each item of category - via app

with t as (

	select 
			if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('home improvement', 'furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
		    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as prod_view_week, 
			*,
			vl.house_id as vlhouse_id
	from (
		select 
			cast(object_id as bigint) as house_id, 
			session_id,
			device_id,
			'app' as source,
			dt
		from l2.mobile_client_event -- app sess 
		where event_type = 'View'
		and object_id is not null and entity_type = 'Product'
		and (context <> 'Back' or context is null)
		and dt >= '2021-03-14'
		) i
	left join shop.vl_pupil vl
	on i.house_id = vl.house_id

)
select category, source, prod_view_week, count(*) as num_prod_views from t
group by 1, 2, 3
;






-- Items added to Cart

with t as (
	select 
			if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('home improvement', 'furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
		    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as cart_add_week, 
		    cast(json_extract(event_metadata, '$.vlid') as bigint) as vlid, 
		    cast(json_extract(event_metadata, '$.qty') as bigint) as cart_item_qty, 
		    * 
	from logs.tmp_marketplace_cart_events_in_web_request_v2 i
	left join shop.vl_pupil vl
	on vl.vendor_listing_id = cast(json_extract(event_metadata, '$.vlid') as bigint)
	where i.event_type = 'cart_add' and i.dt >= '2021-03-14'
--	and i.is_bot = false 
) 
select category, cart_add_week, count(*) as num_cart_adds, sum(cart_item_qty) as total_cart_itemqty from t
group by 1, 2
;






-- Items checked out
with t as (
	select if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('home improvement', 'furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
	       date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.order_date as date)))) as order_week, 
	       * 
	from mp.order_item_margins_with_replacement i
	left join shop.vl_pupil vl
	on vl.vendor_listing_id = i.item_id
	where i.status in (0, 1, 2, 3, 4, 5, 20, 99)
	and i.is_replacement_order = 0
	and i.order_id not in (select order_id from logs.marketplace_gift_cards_purchased)
	and i.order_date >= '2021-03-14'
)
select category, order_week, count(*) as sold_quantity from t
group by 1, 2;




------------- how to convert a date into week starting on Sunday -------
CREATE FUNCTION week_trunc(date, integer DEFAULT 0) RETURNS DATE AS $$
 SELECT DATE_TRUNC('week', $1 + $2)::date - $2
$$ LANGUAGE SQL;


select date_trunc('week' , current_date+parse_duration('1 d'))-parse_duration('1 d'); 