{% test one_active_scd_row(model, column_name) %}

-- SCD Type 2 integrity check: every distinct value of column_name must have
-- exactly one currently-active row (dbt_valid_to is null) at any point in
-- time -- not zero (history was closed but never reopened) and not more
-- than one (a bug let two "current" versions exist simultaneously).

select
    {{ column_name }},
    sum(case when dbt_valid_to is null then 1 else 0 end) as active_row_count
from {{ model }}
group by {{ column_name }}
having active_row_count != 1

{% endtest %}
