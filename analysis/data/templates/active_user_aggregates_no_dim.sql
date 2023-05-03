-- Note that for this query the returned column name must be metric_value for downstream processing
SELECT window_average AS metric_value from (
    SELECT
        *,
        AVG(metric_value) OVER (ORDER BY submission_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
    FROM (
        SELECT
            submission_date,
            app_name,
            SUM({{ metric }}) AS metric_value
        FROM
            `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a
        WHERE
            submission_date >= '{{ start_date }}'
            AND submission_date < '{{ end_date }}'
            AND app_name="{{ app_name }}"
              {% if exclude_dimension_values %}
                {% for dim in exclude_dimension_values -%}
                AND {{dim.dimension}} NOT IN (
                    {{ '\"' + dim.dim_values|join('\", \"') + '\"' }}
                )
                {%- endfor %}
              {% endif %}
              {% if included_dimensions_only %}
               AND (
                    {% for dim in included_dimensions_only -%}
                        {% if loop.index > 1 %}
                            OR
                        {% endif %}
                        {{dim.dimension}} IN ({{ '\"' + dim.dim_values|join('\", \"') + '\"' }})
                {%- endfor %}
                )
              {% endif %}
        GROUP BY
            submission_date,
            app_name
    ) AS t1
    ORDER BY
    submission_date
)
where  submission_date = DATE_SUB(DATE "{{end_date}}", INTERVAL 1 DAY)
