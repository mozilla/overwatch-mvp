-- Note that for this query the returned column name must be metric_value for downstream processing
SELECT
    @full_dim_value_spec,
    window_average AS metric_value
FROM (
    SELECT
        *,
        AVG(metric_value) OVER (
        PARTITION BY @full_dim_spec ORDER BY submission_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
    FROM (
        SELECT
            submission_date,
            @full_dim_spec,
            SUM(@metric) AS metric_value
        FROM
            `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
            `mozdata.static.country_codes_v1` c
        WHERE
            submission_date >= @start_date
            AND submission_date < @end_date
            AND app_name="@app_name"
            AND a.country = c.code
        GROUP BY
            submission_date,
            @full_dim_spec
    ) AS t1
    ORDER BY
        @full_dim_spec,
        submission_date
)
where  submission_date = DATE_SUB(DATE "@window_end_date", INTERVAL 1 DAY)
