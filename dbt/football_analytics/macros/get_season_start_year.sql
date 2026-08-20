{% macro get_season_start_year(date_column) %}
    -- German football seasons run Aug-May, crossing a calendar year
    -- boundary: the season is identified by the year it starts in.
    case
        when month({{ date_column }}) >= 7 then year({{ date_column }})
        else year({{ date_column }}) - 1
    end
{% endmacro %}
