{{ config(materialized='table', schema='bluedropmedical') }}


WITH delivery_times AS (
    SELECT
        delivery_id,
        retailer_id,
        MAX(CASE WHEN event_type = 'DELIVERY_STARTED' THEN event_time END) AS delivery_started_time,
        MAX(CASE WHEN event_type = 'PACKAGE_DELIVERED' THEN event_time END) AS package_delivered_time
    FROM {{ source('bigquery_raw', 'delivery_events') }}
    GROUP BY delivery_id, retailer_id
),

delivery_metrics AS (
    SELECT
        sd.delivery_id,
        sd.retailer_id,
        DATE(DATE_TRUNC(sd.scheduled_time, WEEK(MONDAY))) AS week_start,
        sd.scheduled_time,
        dt.delivery_started_time,
        dt.package_delivered_time,
        -- Calculate delivery duration in minutes
        CASE
            WHEN dt.package_delivered_time IS NOT NULL AND dt.delivery_started_time IS NOT NULL
            THEN TIMESTAMP_DIFF(dt.package_delivered_time, dt.delivery_started_time, MINUTE)
            ELSE NULL
        END AS delivery_duration,
        -- Calculate minutes after scheduled time
        CASE
            WHEN dt.package_delivered_time IS NOT NULL AND sd.scheduled_time IS NOT NULL
            THEN TIMESTAMP_DIFF(dt.package_delivered_time, sd.scheduled_time, MINUTE)
            ELSE NULL
        END AS minutes_after_scheduled,
        -- Determine delivery event status
        CASE
            WHEN dt.delivery_started_time IS NOT NULL AND dt.package_delivered_time IS NOT NULL THEN 'complete'
            WHEN dt.delivery_started_time IS NOT NULL AND dt.package_delivered_time IS NULL THEN 'started_only'
            WHEN dt.delivery_started_time IS NULL AND dt.package_delivered_time IS NOT NULL THEN 'delivered_only'
            ELSE 'missing'
        END AS delivery_event_status
    FROM {{ source('bigquery_raw', 'scheduled_deliveries') }} sd
    LEFT JOIN delivery_times dt
        ON dt.delivery_id = sd.delivery_id
        AND dt.retailer_id = sd.retailer_id
)
-- Final aggregation to produce weekly and retailer-level delivery performance metrics
SELECT
    retailer_id,
    week_start,
    COUNT(delivery_id) AS total_deliveries,
    COUNTIF(minutes_after_scheduled <= 15) AS on_time_deliveries,
    COUNTIF(minutes_after_scheduled > 15) AS late_deliveries,
    AVG(delivery_duration) AS avg_delivery_duration_minutes,
    COUNTIF(delivery_event_status = 'complete') AS complete_deliveries,
    COUNTIF(delivery_event_status = 'started_only') AS started_only_deliveries,
    COUNTIF(delivery_event_status = 'delivered_only') AS delivered_only_deliveries,
    COUNTIF(delivery_event_status = 'missing') AS missing_event_deliveries
FROM delivery_metrics

-- Apply retailer_id filter if provided
WHERE
    {% if var('retailer_id', None) %}
        retailer_id = '{{ var('retailer_id') }}'
    {% else %}
        1=1 -- Include all retailers
    {% endif %}
GROUP BY retailer_id, week_start