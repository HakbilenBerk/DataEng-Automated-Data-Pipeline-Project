class SqlQueries:
    immigration_table_insert = ("""
        SELECT distinct
                id,
                year,
                month,
                country_code,
                city_code,
                arrival_date,
                travel_mode,
                state,
                age,
                gender,
                flight_no  
            FROM staging_immigration
            WHERE id IS NOT NULL
            AND city_code IS NOT NULL
            AND country_code IS NOT NULL
    """)

    temperature_table_insert = ("""
        SELECT distinct
            md5(date || city || country) AS temp_id, 
            date,
            ave_temp,
            city,
            country,
            latitude,
            longitude
        FROM staging_temperature
        WHERE date IS NOT NULL
        AND city IS NOT NULL
        AND country IS NOT NULL       
    """)

    demographics_table_insert = ("""
        SELECT distinct 
            md5(city || state || state_code) AS demo_id, 
            city,
            state,
            median_age,
            male_population,
            female_population,
            total_population,
            foreign_born,
            ave_household_size,
            state_code,
            race,
            count
        FROM staging_demographics
        WHERE city IS NOT NULL
        AND state IS NOT NULL
        AND state_code IS NOT NULL
        AND male_population IS NOT NULL
        AND female_population IS NOT NULL
        AND total_population IS NOT NULL
    """)

    visa_table_insert = ("""
        SELECT distinct 
            id,
            travel_purpose,
            admission_number,
            visa_type,
            visa_expiration
        FROM staging_immigration
        WHERE id IS NOT NULL
        AND visa_type IS NOT NULL
        AND admission_number IS NOT NULL
    """)
