{% macro bearing_degrees_to_cardinal_direction(bearing_degrees, bucket_precision) %}
    {# Convert bearing degrees to cardinal direction with specified precision #}
    case
        when {{ bearing_degrees }} not between 0 and 360
            then 'Please input an accepted value for the first argument "bearing_degrees": range(0, 360)'
        when {{ bucket_precision }} not in (4, 8, 16)
            then 'Please input an accepted value for the second argument "bucket_precision": (4, 8, 16)'

        -- 4 buckets (90° each)
        when {{ bucket_precision }} = 4 then
            case
                when {{ bearing_degrees }} >= 315 or {{ bearing_degrees }} < 45  then 'N'
                when {{ bearing_degrees }} >= 45  and {{ bearing_degrees }} < 135 then 'E'
                when {{ bearing_degrees }} >= 135 and {{ bearing_degrees }} < 225 then 'S'
                when {{ bearing_degrees }} >= 225 and {{ bearing_degrees }} < 315 then 'W'
            end

        -- 8 buckets (45° each)
        when {{ bucket_precision }} = 8 then
            case
                when {{ bearing_degrees }} >= 337.5 or {{ bearing_degrees }} < 22.5  then 'N'
                when {{ bearing_degrees }} >= 22.5  and {{ bearing_degrees }} < 67.5  then 'NE'
                when {{ bearing_degrees }} >= 67.5  and {{ bearing_degrees }} < 112.5 then 'E'
                when {{ bearing_degrees }} >= 112.5 and {{ bearing_degrees }} < 157.5 then 'SE'
                when {{ bearing_degrees }} >= 157.5 and {{ bearing_degrees }} < 202.5 then 'S'
                when {{ bearing_degrees }} >= 202.5 and {{ bearing_degrees }} < 247.5 then 'SW'
                when {{ bearing_degrees }} >= 247.5 and {{ bearing_degrees }} < 292.5 then 'W'
                when {{ bearing_degrees }} >= 292.5 and {{ bearing_degrees }} < 337.5 then 'NW'
            end

        -- 16 buckets (22.5° each)
        when {{ bucket_precision }} = 16 then
            case
                when {{ bearing_degrees }} >= 348.75 or {{ bearing_degrees }} < 11.25  then 'N'
                when {{ bearing_degrees }} >= 11.25  and {{ bearing_degrees }} < 33.75  then 'NNE'
                when {{ bearing_degrees }} >= 33.75  and {{ bearing_degrees }} < 56.25  then 'NE'
                when {{ bearing_degrees }} >= 56.25  and {{ bearing_degrees }} < 78.75  then 'ENE'
                when {{ bearing_degrees }} >= 78.75  and {{ bearing_degrees }} < 101.25 then 'E'
                when {{ bearing_degrees }} >= 101.25 and {{ bearing_degrees }} < 123.75 then 'ESE'
                when {{ bearing_degrees }} >= 123.75 and {{ bearing_degrees }} < 146.25 then 'SE'
                when {{ bearing_degrees }} >= 146.25 and {{ bearing_degrees }} < 168.75 then 'SSE'
                when {{ bearing_degrees }} >= 168.75 and {{ bearing_degrees }} < 191.25 then 'S'
                when {{ bearing_degrees }} >= 191.25 and {{ bearing_degrees }} < 213.75 then 'SSW'
                when {{ bearing_degrees }} >= 213.75 and {{ bearing_degrees }} < 236.25 then 'SW'
                when {{ bearing_degrees }} >= 236.25 and {{ bearing_degrees }} < 258.75 then 'WSW'
                when {{ bearing_degrees }} >= 258.75 and {{ bearing_degrees }} < 281.25 then 'W'
                when {{ bearing_degrees }} >= 281.25 and {{ bearing_degrees }} < 303.75 then 'WNW'
                when {{ bearing_degrees }} >= 303.75 and {{ bearing_degrees }} < 326.25 then 'NW'
                when {{ bearing_degrees }} >= 326.25 and {{ bearing_degrees }} < 348.75 then 'NNW'
            end
    end
{% endmacro %}

