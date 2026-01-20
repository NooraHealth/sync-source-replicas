-- @dataset_id: hrpw_8th_month_dataset
-- @enumerators: rahul.a@noorahealth.org,neelkandan@noorahealth.org
SELECT
    id,
    name,
    mobile_number,
    expected_date_of_delivery,
    CURRENT_TIMESTAMP AS ingested_at
FROM users
WHERE program_id = (SELECT id FROM noora_programs WHERE name = 'hrpws')
    AND condition_area_id = (SELECT id FROM condition_areas WHERE name = 'hrpw')
    AND (consented_at IS NOT NULL OR whatsapp_onboarding_date IS NOT NULL)
