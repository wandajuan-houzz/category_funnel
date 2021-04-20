-- join queries developed from v1 to generate concatenated table for the complete category funnel



select pv.*, ca.num_cart_adds, ca.total_cart_itemqty, chkout.sold_quantity from (
	with t as (
	
		select 
				if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
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
			from l2.page_views_daily -- web sess
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
	select category, prod_view_week, count(*) as num_prod_views from t
	group by 1, 2
	) pv,

	(
	with t as (
		select 
				if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
			    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as cart_add_week, 
			    cast(json_extract(event_metadata, '$.vlid') as bigint) as vlid, 
			    cast(json_extract(event_metadata, '$.qty') as bigint) as cart_item_qty, 
			    * 
		from logs.tmp_marketplace_cart_events_in_web_request_v2 i
		left join shop.vl_pupil vl
		on vl.vendor_listing_id = cast(json_extract(event_metadata, '$.vlid') as bigint)
		where i.event_type = 'cart_add' and i.dt >= '2021-03-14'
--		and i.is_bot = false 
	) 
	select category, cart_add_week, count(*) as num_cart_adds, sum(cart_item_qty) as total_cart_itemqty from t
	group by 1, 2
	) ca,
	
	(
	with t as (
		select if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
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
	group by 1, 2
	) chkout
	
where pv.category = ca.category and pv.prod_view_week = ca.cart_add_week
and pv.category = chkout.category and pv.prod_view_week = chkout.order_week
;



with t as (
	select 
			if(vl.l1_category_name is null, 'N/A', if(vl.l1_category_name in ('furniture', 'home decor', 'lighting', 'outdoor products', 'bath products'), vl.l1_category_name, 'others')) as category,
		    date_add('day', -1, date_trunc('week', date_add('day', 1, cast(i.dt as date)))) as cart_add_week, 
		    cast(json_extract(event_metadata, '$.vlid') as bigint) as vlid, 
		    cast(json_extract(event_metadata, '$.qty') as bigint) as cart_item_qty, 
		    * 
	from logs.tmp_marketplace_cart_events_in_web_request_v2 i
	left join shop.vl_pupil vl
	on vl.vendor_listing_id = cast(json_extract(event_metadata, '$.vlid') as bigint)
	where i.event_type = 'cart_add' and i.dt >= '2021-03-14'
	and i.is_bot = false 
) 
select category, cart_add_week, count(*) as num_cart_adds, sum(cart_item_qty) as total_cart_itemqty from t
group by 1, 2
;