-- Note that for this query the returned column name must be metric_value for downstream processing
SELECT window_average AS metric_value from (
    SELECT
        *,
        AVG(metric_value) OVER (ORDER BY submission_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
    FROM (
        SELECT
            date as submission_date,
            sum(@metric) AS metric_value
        FROM
            `moz-fx-data-marketing-prod.ga_derived.www_site_metrics_summary_v1`
        WHERE
            date >= @start_date
            AND date < @end_date
        GROUP BY date
        ORDER BY date desc
    ) AS t1
    ORDER BY
    submission_date
)
where  submission_date = DATE_SUB(DATE "@window_end_date", INTERVAL 1 DAY)
