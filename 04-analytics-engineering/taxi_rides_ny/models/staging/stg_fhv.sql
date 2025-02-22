select *
from {{ source('staging', 'fhv_bq') }}
where dispatching_base_num is not null