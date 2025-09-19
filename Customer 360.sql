with conversion_1 as (select
    cd.customer_id,
    cs.conversion_date,
    row_number() over(partition by cs.fk_customer order by conversion_date)AS recurrence,
    lead(conversion_date)over(partition by cs.fk_customer order by conversion_date) AS next_conversion_date,
   cs.conversion_type,
   cs.conversion_channel,
   cs.fk_customer,
    cs.conversion_id,
    od.order_date as first_order_date,
    dd.year_week as first_order_week,
    od.order_id as first_order_id,
    pd.product_name as first_order_product,
    od.price_paid as first_order_total_paid,
    od.discount as first_order_discount
from fact_tables.conversions as cs
left join dimensions.customer_dimension as cd
on cs.fk_customer=cd.sk_customer
left join fact_tables.orders as od
on od.order_number=cs.order_number
left join dimensions.date_dimension as dd
on od.order_date=dd.date
left join dimensions.product_dimension as pd
on pd.sk_product = od.fk_product),
    dt as (
    select dd.year_week,
           dd.date
from dimensions.date_dimension as dd),
    pb_1 as (select
                 dt.year_week as order_week,
                conversion_1.*
             from dt
                      right join conversion_1
                      on dt.year_week >= conversion_1.first_order_week
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),
    pb_2 as (select dt.date as order_date,
            pb_1.fk_customer
             from dt left join pb_1
            on dt.date >= pb_1.first_order_date
            group by 1,2),

    wr_1 as (select
               sum(od.price_paid)  as week_revenue,
               pb.order_week,
               pb.fk_customer
               from fact_tables.orders as od
               right join pb_1 as pb
               on pb.fk_customer = od.fk_customer
             group by 2,3),
    wd_1 as (select
               sum(od.discount) as week_discounts,
               pb.order_week,
               pb.fk_customer
               from fact_tables.orders as od
               right join pb_1 as pb
               on pb.fk_customer = od.fk_customer
               group by 2,3),
    cr_1 as (select dd.date,
                    sum(od.price_paid)over (ORDER BY dd.date) AS cumulative_revenue
                 from dimensions.date_dimension as dd
                 left join fact_tables.orders as od
                 on dd.date >= od.order_date),
    ly_1 as (select dd.date,
                    count(od.order_number)over (ORDER BY dd.date) AS loyalty
                 from dimensions.date_dimension as dd
                 left join fact_tables.orders as od
                 on dd.date >= od.order_date)
select * from pb_1




