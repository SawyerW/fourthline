{{ config(materialized='table') }}

with
decision as (
select * from {{ source('main', 'decision') }}
)
select
    json_extract(raw, '$.request_id') as request_id
,   json_extract(raw, '$.timestamp') as timestamp
,   json_extract(raw, '$.agent_id') as agent_id
,   json_extract(raw, '$.is_fraud_request') as is_fraud_request
from decision