-- Calendar with the Indian kharif marketing year (Oct-Sep), when basmati is harvested and sold.
select
    d::date                                         as date_day,
    year(d)::integer                                as year,
    quarter(d)::integer                             as quarter,
    month(d)::integer                               as month,
    monthname(d)                                    as month_name,
    date_trunc('month', d)::date                    as month_start,
    isodow(d)::integer                              as iso_day_of_week,
    isodow(d) in (6, 7)                             as is_weekend,
    case when month(d) >= 10 then year(d) else year(d) - 1 end::integer
                                                    as kharif_marketing_year
from range(
    date '{{ var("date_spine_start") }}',
    date '{{ var("date_spine_end") }}' + interval 1 day,
    interval 1 day
) as t(d)
