DROP TABLE IF EXISTS public.staging_immigration;
DROP TABLE IF EXISTS public.staging_temperature;
DROP TABLE IF EXISTS public.staging_demographics;
DROP TABLE IF EXISTS public.visa_details;
DROP TABLE IF EXISTS public.immigration;
DROP TABLE IF EXISTS public.temperature;
DROP TABLE IF EXISTS public.demographics;


CREATE TABLE IF NOT EXISTS public.visa_details (
    visa_id NUMERIC NOT NULL,
    travel_purpose NUMERIC,
    admission_number NUMERIC,
    visa_type VARCHAR,
    visa_expiration VARCHAR,
    CONSTRAINT visa_pkey PRIMARY KEY(visa_id)   
);

CREATE TABLE IF NOT EXISTS public.immigration (
	immigration_id NUMERIC NOT NULL,
	year NUMERIC,
	month NUMERIC,
	country_code NUMERIC,
	city_code VARCHAR,
    arrival_date NUMERIC,
    travel_mode NUMERIC,
    state VARCHAR,
    age NUMERIC,
    gender VARCHAR,
    flight_no VARCHAR,
    CONSTRAINT immigration_pkey PRIMARY KEY(immigration_id)  
);

CREATE TABLE IF NOT EXISTS public.temperature (
    temp_id VARCHAR NOT NULL,
    date DATE,
    ave_temp NUMERIC,
    city VARCHAR,
    country VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    CONSTRAINT temp_pkey PRIMARY KEY(temp_id)
);


CREATE TABLE IF NOT EXISTS public.demographics (
    demo_id VARCHAR NOT NULL,
    city VARCHAR,
    state VARCHAR,
    median_age NUMERIC,
    male_population NUMERIC,
    female_population NUMERIC,
    total_population NUMERIC,
    foreign_born NUMERIC,
    ave_household_size NUMERIC,
    state_code VARCHAR,
    race VARCHAR,
    count NUMERIC,
    CONSTRAINT demo_pkey PRIMARY KEY(demo_id)
);

CREATE TABLE IF NOT EXISTS public.staging_immigration (
	id NUMERIC,
	year NUMERIC,
	month NUMERIC,
	country_code NUMERIC,
	city_code VARCHAR,
    arrival_date NUMERIC,
    travel_mode NUMERIC,
    state VARCHAR,
    age NUMERIC,
    travel_purpose NUMERIC,
    gender VARCHAR,
    flight_no VARCHAR,
    admission_number NUMERIC,
    visa_type VARCHAR,
    visa_expiration VARCHAR    
);

CREATE TABLE IF NOT EXISTS public.staging_temperature (
    date DATE,
    ave_temp NUMERIC,
    city VARCHAR,
    country VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
);

CREATE TABLE IF NOT EXISTS public.staging_demographics (
    city VARCHAR,
    state VARCHAR,
    median_age NUMERIC,
    male_population NUMERIC,
    female_population NUMERIC,
    total_population NUMERIC,
    foreign_born NUMERIC,
    ave_household_size NUMERIC,
    state_code VARCHAR,
    race VARCHAR,
    count NUMERIC
);






