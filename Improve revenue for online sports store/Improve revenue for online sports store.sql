/*Counting missing values*/
with count_total as
    (select
        count(*) as total_rows
    from info),
count_des as
    (select
        count(description) as count_description
    from info
    where description is not null),
count_list as
    (select
        count(listing_price) as count_listing_price
    from finance
    where listing_price is not null),
count_last as
    (select
        count(last_visited) as count_last_visited
    from traffic
    where last_visited is not null)
select
    ct.total_rows,
    cd.count_description,
    cli.count_listing_price,
    cla.count_last_visited
from count_total as ct
    cross join count_des as cd
    cross join count_list as cli
    cross join count_last as cla;

/*count all non-missing brand products and group them by their price */
select
    b.brand,
    cast(f.listing_price as int),
    count(f.product_id)
from brands as b
    join finance as f
    on b.product_id = f.product_id
    where listing_price > 0
    group by b.brand, f.listing_price
    order by listing_price desc;
    
/*Labeling the price ranges of the products*/
select
    b.brand,
    count(f.product_id),
    sum(f.revenue) as total_revenue,
    case when listing_price < 42 then 'Budget'
        when listing_price >= 42 and listing_price < 74 then 'Average'
        when listing_price >= 74 and listing_price < 129 then 'Expensive'
        when listing_price >= 129 then 'Elite'
        end as price_category
from brands as b
    inner join finance as f
    on b.product_id = f.product_id
where b.brand is not null
group by brand, price_category
order by total_revenue desc;

/*calculate the average discount of different brands*/
select
    b.brand,
    concat(avg(f.discount)*100,'%') as average_discount
from brands as b
    join finance as f
    on b.product_id = f.product_id
where b.brand is not null
group by b.brand;

/*Correlation between revenue and reviews*/
select
    corr(r.reviews,f.revenue) as review_revenue_corr
from reviews as r
    join finance as f
    on r.product_id = f.product_id;
    
/*Ratings and reviews by product description length*/
select
    trunc(length(i.description), -2) as description_length,
    round(avg(cast(r.rating as numeric)),2) as average_rating
from info as i
    join reviews as r
    on i.product_id = r.product_id
where i.description is not null
group by description_length
order by description_length;

/*Reviews by month and brand*/
select
    b.brand,
    extract(month from t.last_visited) as month,
    count(r.*) as num_reviews
from traffic as t
    join brands as b
    on t.product_id = b.product_id
    join reviews as r
    on t.product_id = r.product_id
where brand is not null
group by b.brand, month
order by b.brand, month;

/*Footwear product performance*/
with footwear as
    (select 
        i.description,
         f.revenue
    from info as i
         join finance as f
         on i.product_id = f.product_id
    where description like '%shoe%'
        or description like '%trainer%'
        or description like '%foot%'
        and description is not null)
select 
    count(description) as num_footwear_products,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) as median_footwear_revenue
    from footwear;
    
/*other products performance*/
with footwear as
    (select 
        i.description,
         f.revenue
    from info as i
         join finance as f
         on i.product_id = f.product_id
    where description like '%shoe%'
        or description like '%trainer%'
        or description like '%foot%'
        and description is not null)
select
    count(i.*) as num_clothing_products,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) as median_clothing_revenue
from info as i
    join finance as f
    on i.product_id = f.product_id
where description not in (select description from footwear);

