-- Note that for this query the returned column name must be metric_value for downstream processing
SELECT
    {{ full_dim_value_spec }},
    window_average AS metric_value
FROM (
    SELECT
        *,
        AVG(metric_value) OVER (
        PARTITION BY {{ full_dim_spec }} ORDER BY submission_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
    FROM (
        SELECT
            date as submission_date,
            {{ full_dim_spec }},
            SUM({{ metric }}) AS metric_value
        FROM
            `moz-fx-data-marketing-prod.ga_derived.www_site_metrics_summary_v1`
        WHERE
            date >= '{{ start_date }}'
            AND date < '{{ end_date }}'
        GROUP BY
            date,
            {{ full_dim_spec }}

    ) AS t1
    ORDER BY
        {{ full_dim_spec }},
        submission_date
)
where  submission_date = DATE_SUB(DATE "{{end_date}}", INTERVAL 1 DAY)
