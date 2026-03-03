-- dataset_id: hrpw_8th_month_dataset
SELECT id, name, mobile_number, expected_date_of_delivery, CURRENT_TIMESTAMP AS ingested_at
FROM users
WHERE program_id = (SELECT id FROM noora_programs WHERE name = 'high_risk_referral')
	AND expected_date_of_delivery
    BETWEEN (CURRENT_DATE + INTERVAL '1 month')
      AND (CURRENT_DATE + INTERVAL '2 months')
	AND COALESCE(consented_at, whatsapp_onboarding_date) is not NULL

-- dataset_id: hrpw_8th_month_dataset1
SELECT id, name, mobile_number, expected_date_of_delivery, CURRENT_TIMESTAMP AS ingested_at
FROM users
WHERE program_id = (SELECT id FROM noora_programs WHERE name = 'high_risk_referral')
	AND expected_date_of_delivery
    BETWEEN (CURRENT_DATE + INTERVAL '1 month')
      AND (CURRENT_DATE + INTERVAL '2 months')
	AND COALESCE(consented_at, whatsapp_onboarding_date) is not NULL
