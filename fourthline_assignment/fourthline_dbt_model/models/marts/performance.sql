
{{ config(materialized='table') }}

with
prediction as (
select * from {{ ref('base_prediction') }}
),
decision as (
select * from {{ ref('base_decision') }}
)
select 
    p.request_timestamp as insert_timestamp
,   p.request_id as request_id
,   d.agent_id as agent_id
,   p.probability_of_fraud as probability_of_fraud
,   d.is_fraud_request as final_decision_by_agent
,   case when p.probability_of_fraud <= 0.7 then 0 else 1 end as prediction_decision
from prediction p
left join decision as d
on p.request_id = d.request_id
