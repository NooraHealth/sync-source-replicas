-- @dataset_id: hrpw_8th_month_dataset
-- @enumerators: rahul.a@noorahealth.org,neelkandan@noorahealth.org
SELECT id, name, mobile_number, expected_date_of_delivery, CURRENT_TIMESTAMP AS ingested_at
FROM users
WHERE program_id = (SELECT id FROM noora_programs WHERE name = 'high_risk_referral')
	AND expected_date_of_delivery
    	BETWEEN (CURRENT_DATE + INTERVAL '1 month')
        AND (CURRENT_DATE + INTERVAL '2 months')
	AND (consented_at is not NULL OR whatsapp_onboarding_date is not NULL)
