

-- sheet_name: bangladesh_facilities
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.bn') as facility_name_bangla,
      f.state_id as division_id,
      s.name as division_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_state` as s
      on f.state_id = s.id
    left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select *
from facilities
order by facility_name;

-- sheet_name: bangladesh_trainers
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.bn') as facility_name_bangla,
      f.state_id as division_id,
      s.name as division_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_state` as s
      on f.state_id = s.id
    left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select
  t.id as trainer_id,
  t.name as trainer_name,
  t.department_id,
  dp.name as department_name,
  t.designation_id,
  dg.name as designation_name,
  f.* except (is_active, created_at, updated_at),
  t.account_status,
  t.date_of_tot,
  t.created_at,
  t.updated_at
from `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_nurseprofile` as t
left join facilities as f
  using (facility_id)
left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_department` as dp
  on t.department_id = dp.id
left join `noorahealth-raw`.`hep_bangladesh_unified`.`app_backend_designation` as dg
  on t.designation_id = dg.id
where f.facility_id is not null
order by f.facility_name, lower(t.name), t.id;

-- sheet_name: india_facilities
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      f.state_id,
      s.name as state_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_india_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_india_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_india_unified`.`app_backend_state` as s
      on f.state_id = s.id
    left join `noorahealth-raw`.`hep_india_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select *
from facilities
order by facility_name;

-- sheet_name: india_trainers
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      f.state_id,
      s.name as state_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_india_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_india_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_india_unified`.`app_backend_state` as s
      on f.state_id = s.id
    left join `noorahealth-raw`.`hep_india_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select
  t.id as trainer_id,
  t.name as trainer_name,
  t.department_id,
  dp.name as department_name,
  t.designation_id,
  dg.name as designation_name,
  f.* except (is_active, created_at, updated_at),
  t.account_status,
  t.date_of_tot,
  t.created_at,
  t.updated_at
from `noorahealth-raw`.`hep_india_unified`.`app_backend_nurseprofile` as t
left join facilities as f
  using (facility_id)
left join `noorahealth-raw`.`hep_india_unified`.`app_backend_department` as dp
  on t.department_id = dp.id
left join `noorahealth-raw`.`hep_india_unified`.`app_backend_designation` as dg
  on t.designation_id = dg.id
where f.facility_id is not null
order by f.facility_name, lower(t.name), t.id;

-- sheet_name: indonesia_facilities
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.id') as facility_name_indonesian,
      f.province_id,
      p.name as province_name,
      f.regency_id,
      r.name as regency_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_province` as p
      on f.province_id = p.id
    left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_regency` as r
      on f.regency_id = r.id
)
select *
from facilities
order by facility_name;

-- sheet_name: indonesia_trainers
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.id') as facility_name_indonesian,
      f.province_id,
      p.name as province_name,
      f.regency_id,
      r.name as regency_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_province` as p
      on f.province_id = p.id
    left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_regency` as r
      on f.regency_id = r.id
)
select
  t.id as trainer_id,
  t.name as trainer_name,
  t.department_id,
  dp.name as department_name,
  t.designation_id,
  dg.name as designation_name,
  f.* except (is_active, created_at, updated_at),
  t.account_status,
  t.date_of_tot,
  t.created_at,
  t.updated_at
from `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_nurseprofile` as t
left join facilities as f
  using (facility_id)
left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_department` as dp
  on t.department_id = dp.id
left join `noorahealth-raw`.`hep_indonesia_unified`.`app_backend_designation` as dg
  on t.designation_id = dg.id
where f.facility_id is not null
order by f.facility_name, lower(t.name), t.id;

-- sheet_name: nepal_facilities
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.ne') as facility_name_nepali,
      f.province_id,
      p.name as province_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_nepal_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_province` as p
      on f.province_id = p.id
    left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select *
from facilities
order by facility_name;

-- sheet_name: nepal_trainers
with facilities as (
  select
    f.id as facility_id,
    f.name as facility_name,
      json_value(f.localized_name, '$.ne') as facility_name_nepali,
      f.province_id,
      p.name as province_name,
      f.district_id,
      d.name as district_name,
    f.facility_type_id,
    t.name as facility_type_name,
    f.is_active,
    f.test_facility,
    f.created_at,
    f.updated_at
  from `noorahealth-raw`.`hep_nepal_unified`.`app_backend_facility` as f
  left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_facilitytype` as t
    on f.facility_type_id = t.id
    left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_province` as p
      on f.province_id = p.id
    left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_district` as d
      on f.district_id = d.id
)
select
  t.id as trainer_id,
  t.name as trainer_name,
  t.department_id,
  dp.name as department_name,
  t.designation_id,
  dg.name as designation_name,
  f.* except (is_active, created_at, updated_at),
  t.account_status,
  t.date_of_tot,
  t.created_at,
  t.updated_at
from `noorahealth-raw`.`hep_nepal_unified`.`app_backend_nurseprofile` as t
left join facilities as f
  using (facility_id)
left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_department` as dp
  on t.department_id = dp.id
left join `noorahealth-raw`.`hep_nepal_unified`.`app_backend_designation` as dg
  on t.designation_id = dg.id
where f.facility_id is not null
order by f.facility_name, lower(t.name), t.id;
