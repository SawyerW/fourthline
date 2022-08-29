

{{ config(materialized='table') }}

with
prediction as (
select * from {{ source('main', 'prediction') }}
)
select
    json_extract(raw, '$.request_id') as request_id
,   json_extract(raw, '$.request_timestamp') as request_timestamp
,   json_extract(raw, '$.latitude') as latitude
,   json_extract(raw, '$.longitude') as longitude
,   json_extract(raw, '$.document_photo_brightness_percent') as document_photo_brightness_percent
,   json_extract(raw, '$.is_photo_in_a_photo_selfie') as is_photo_in_a_photo_selfie
,   json_extract(raw, '$.document_matches_selfie_percent') as document_matches_selfie_percent
,   json_extract(raw, '$.s3_path_to_selfie_photo') as s3_path_to_selfie_photo
,   json_extract(raw, '$.s3_path_to_document_photo') as s3_path_to_document_photo
,   json_extract(raw, '$.probability_of_fraud') as probability_of_fraud
from prediction