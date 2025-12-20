/*
===============================================================================
PROCEDURE: silver.usp_LoadSilverLayer
PURPOSE: Load and transform data from bronze to silver layer
DESCRIPTION:
    This procedure performs the ETL process to load cleaned, validated data
    from the bronze layer into the silver layer. It applies business rules,
    data type conversions, and quality checks while maintaining referential
    integrity and audit trails.
    
PROCESS FLOW:
    1. Validate parameters and initialize logging
    2. Begin transaction for data consistency
    3. TRUNCATE/DELETE silver tables to avoid duplication
    4. Sequentially load each table with transformations
    5. Commit transaction on success
    6. Rollback on error with detailed logging
    
PARAMETERS:
    @LoadDate    - Optional date for incremental loading (default: current date)
    @DebugMode   - Enable detailed logging (0=off, 1=on)
    
RETURNS:
    0  - Success
    -1 - Failure with error details
    
DEPENDENCIES:
    - All bronze layer tables must exist
    - Silver layer DDL must be executed first
    
AUTHOR: Kevin Junior
CREATED: 2025-12-19
LAST MODIFIED: 2025-12-20
VERSION: 2.3 - Simplified constraint handling, using DELETE for constrained tables
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.usp_LoadSilverLayer
    @LoadDate DATE = NULL,
    @DebugMode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variable declarations
    DECLARE @ProcedureName NVARCHAR(100) = 'usp_LoadSilverLayer';
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowsAffected INT = 0;
    DECLARE @ErrorMsg NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    
    -- Set default load date if not provided
    IF @LoadDate IS NULL
        SET @LoadDate = CAST(GETDATE() AS DATE);
    
    BEGIN TRY
        -- Begin transaction for data consistency
        BEGIN TRANSACTION;
        
        -- Log start of process
        PRINT REPLICATE('=', 80);
        PRINT 'SILVER LAYER LOAD PROCESS INITIATED';
        PRINT 'Procedure: ' + @ProcedureName;
        PRINT 'Load Date: ' + CAST(@LoadDate AS NVARCHAR(20));
        PRINT 'Start Time: ' + FORMAT(@StartTime, 'yyyy-MM-dd HH:mm:ss');
        PRINT 'Mode: Full reload (TRUNCATE/DELETE and INSERT)';
        PRINT REPLICATE('=', 80);
        
        -- --------------------------------------------------------------------
        -- IMPORTANT: Load order matters for referential integrity
        -- 1. First truncate/dependent tables (child tables)
        -- 2. Then truncate/parent tables
        -- 3. Load parent tables first
        -- 4. Then load child tables
        -- --------------------------------------------------------------------
        
        -- --------------------------------------------------------------------
        -- PHASE 1: TRUNCATE/DELETE ALL SILVER TABLES
        -- --------------------------------------------------------------------
        PRINT 'PHASE 1: Clearing all silver tables...';
        
        -- 1. Clear child tables first (tables with foreign keys)
        PRINT '   1. Clearing child tables...';
        
        -- Clear competitor_stores first (depends on competitors)
        PRINT '      • Clearing silver.competitor_stores...';
        TRUNCATE TABLE silver.competitor_stores;
        
        -- 2. Clear parent tables (tables referenced by others)
        PRINT '   2. Clearing parent tables...';
        
        -- Clear all other tables (no foreign key dependencies)
        PRINT '      • Clearing silver.crm...';
        TRUNCATE TABLE silver.crm;
        
        PRINT '      • Clearing silver.pos...';
        TRUNCATE TABLE silver.pos;
        
        PRINT '      • Clearing silver.stores...';
        TRUNCATE TABLE silver.stores;
        
        PRINT '      • Clearing silver.gis_counties...';
        TRUNCATE TABLE silver.gis_counties;
        
        PRINT '      • Clearing silver.gis_locations...';
        TRUNCATE TABLE silver.gis_locations;
        
        PRINT '      • Clearing silver.economic...';
        TRUNCATE TABLE silver.economic;
        
        -- 3. Clear competitors table (has dependent table competitor_stores)
        PRINT '      • Clearing silver.competitors (using DELETE due to foreign key)...';
        DELETE FROM silver.competitors;  -- Use DELETE instead of TRUNCATE
        
        PRINT '   All silver tables cleared successfully.';
        
        -- --------------------------------------------------------------------
        -- PHASE 2: LOAD DATA WITH PROPER REFERENTIAL ORDER
        -- --------------------------------------------------------------------
        PRINT 'PHASE 2: Loading data with referential integrity...';
        
        -- --------------------------------------------------------------------
        -- 1. LOAD CRM DATA (No dependencies)
        -- --------------------------------------------------------------------
        PRINT '1. Loading CRM Customer Data...';
        
        WITH DeduplicatedCRM AS (
            SELECT 
                -- Standardize customer ID format
                'CUST_' + SUBSTRING(customer_id, 6, 10) AS customer_id,
                
                -- Personal information
                first_name, 
                last_name, 
                gender,
                
                -- Contact information with NULL handling
                ISNULL(phone, 'unknown') AS phone,
                ISNULL(email, 'email@unknown') AS email,
                
                -- Geographic information
                county, 
                area, 
                customer_segment,
                
                -- Date parsing with European format (DD-MM-YYYY)
                TRY_CONVERT(DATE, registration_date, 105) AS registration_date,
                customer_status,
                TRY_CONVERT(DATE, last_purchase_date, 105) AS last_purchase_date,
                
                -- Behavioral metrics
                purchase_frequency_monthly,
                CAST(avg_transaction_value_kes AS DECIMAL(18,2)) AS avg_transaction_value_kes,
                CAST(lifetime_value_kes AS DECIMAL(18,2)) AS lifetime_value_kes,
                
                -- Preferences
                preferred_store_format, 
                communication_preferences,
                
                -- Feedback score with type casting
                CAST(feedback_score AS DECIMAL(3,1)) AS feedback_score,
                
                -- Data provenance
                data_source, 
                extracted_date, 
                record_version,
                
                -- Deduplication: Keep earliest registration
                ROW_NUMBER() OVER (
                    PARTITION BY 'CUST_' + SUBSTRING(customer_id, 6, 10) 
                    ORDER BY registration_date
                ) AS duplicate_rank
            FROM bronze.crm_raw
        )
        INSERT INTO silver.crm (
            customer_id, first_name, last_name, gender, phone, email,
            county, area, customer_segment, registration_date, customer_status,
            last_purchase_date, purchase_frequency_monthly, avg_transaction_value_kes,
            lifetime_value_kes, preferred_store_format, communication_preferences,
            feedback_score, data_source, extracted_date, record_version
        )
        SELECT 
            customer_id, first_name, last_name, gender, phone, email,
            county, area, customer_segment, registration_date, customer_status,
            last_purchase_date, purchase_frequency_monthly, avg_transaction_value_kes,
            lifetime_value_kes, preferred_store_format, communication_preferences,
            feedback_score, data_source, extracted_date, record_version
        FROM DeduplicatedCRM
        WHERE duplicate_rank = 1;  -- Remove duplicates
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 2. LOAD POS TRANSACTION DATA (Depends on CRM and stores)
        -- --------------------------------------------------------------------
        PRINT '2. Loading POS Transaction Data...';
        
        INSERT INTO silver.pos (
            transaction_id, transaction_date, store_id, store_county, store_format,
            customer_id, line_item_id, product_id, category, subcategory,
            product_name, unit_price_kes, quantity, total_price_kes,
            discount_kes, final_price_kes, payment_method, staff_id,
            data_source, extracted_timestamp
        )
        SELECT
            -- Transaction identifiers
            transaction_id,
            
            -- Date parsing with multiple format support
            CASE 
                WHEN transaction_date LIKE '____-__-__ __:__:__'  -- ISO format
                    THEN TRY_CONVERT(DATE, TRY_CONVERT(DATETIME, transaction_date, 120))
                WHEN transaction_date LIKE '__/__/____ __:__:__%' -- European with slashes
                    THEN TRY_CONVERT(DATE, TRY_CONVERT(DATETIME, transaction_date, 103))
                WHEN transaction_date LIKE '__-__-____ __:__:__%' -- European with dashes
                    THEN TRY_CONVERT(DATE, TRY_CONVERT(DATETIME, transaction_date, 105))
                ELSE TRY_CONVERT(DATE, transaction_date)          -- Generic attempt
            END AS transaction_date,
            
            -- Store information
            store_id,
            store_county,
            store_format,
            
            -- Customer handling (NULL becomes ANONYMOUS)
            CASE 
                WHEN customer_id IS NULL 
                THEN 'ANONYMOUS'
                ELSE customer_id
            END AS customer_id,
            
            -- Product details
            line_item_id,
            product_id,
            category,
            subcategory,
            product_name,
            
            -- Price validation and type casting
            CASE 
                WHEN TRY_CAST(unit_price_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                WHEN TRY_CAST(unit_price_kes AS DECIMAL(18,2)) < 0 THEN 0.0
                ELSE CAST(unit_price_kes AS DECIMAL(18,2))
            END AS unit_price_kes,
            
            -- Quantity validation
            CASE 
                WHEN TRY_CAST(quantity AS INT) IS NULL THEN 0
                WHEN TRY_CAST(quantity AS INT) < 0 THEN 0
                ELSE CAST(quantity AS INT)
            END AS quantity,
            
            -- Total price
            CASE 
                WHEN TRY_CAST(total_price_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(total_price_kes AS DECIMAL(18,2))
            END AS total_price_kes,
            
            -- Discount validation (cannot exceed total price)
            CASE 
                WHEN TRY_CAST(discount_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                WHEN TRY_CAST(discount_kes AS DECIMAL(18,2)) < 0 THEN 0.0
                WHEN TRY_CAST(discount_kes AS DECIMAL(18,2)) > 
                     TRY_CAST(total_price_kes AS DECIMAL(18,2))
                    THEN CAST(total_price_kes AS DECIMAL(18,2))
                ELSE CAST(discount_kes AS DECIMAL(18,2))
            END AS discount_kes,
            
            -- Final price validation
            CASE 
                WHEN TRY_CAST(final_price_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                WHEN TRY_CAST(final_price_kes AS DECIMAL(18,2)) < 0 THEN 0.0
                ELSE CAST(final_price_kes AS DECIMAL(18,2))
            END AS final_price_kes,
            
            -- Payment method standardization
            CASE 
                WHEN LOWER(LTRIM(RTRIM(payment_method))) = 'mpesa' THEN 'M-Pesa'
                WHEN LOWER(LTRIM(RTRIM(payment_method))) = 'cash' THEN 'Cash'
                WHEN LOWER(LTRIM(RTRIM(payment_method))) = 'card' THEN 'Card'
                WHEN payment_method IS NULL OR LTRIM(RTRIM(payment_method)) = '' 
                    THEN 'Unknown'
                ELSE payment_method
            END AS payment_method,
            
            -- Additional transaction details
            staff_id,
            data_source,
            extracted_timestamp
        FROM bronze.pos_raw
        WHERE -- Data quality filter
            (TRY_CAST(final_price_kes AS DECIMAL(18,2)) >= 0
             OR TRY_CAST(final_price_kes AS DECIMAL(18,2)) IS NULL);
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 3. LOAD STORES DATA (No dependencies)
        -- --------------------------------------------------------------------
        PRINT '3. Loading Store Locations Data...';
        
        INSERT INTO silver.stores (store_id, store_name, county, format, size_sqm)
        SELECT 
            store_id, 
            store_name, 
            county, 
            format,
            CAST(size_sqm AS INT) AS size_sqm
        FROM bronze.stores_raw;
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 4. LOAD GIS COUNTIES DATA (No dependencies)
        -- --------------------------------------------------------------------
        PRINT '4. Loading GIS County Demographic Data...';
        
        INSERT INTO silver.gis_counties (
            county_id, county_name, population_2023, area_sqkm,
            population_density_psqkm, poverty_rate, unemployment_rate,
            avg_household_income_kes, urbanization_rate, literacy_rate,
            road_infrastructure_score, public_transport_score,
            internet_penetration, commercial_rent_kes_psqm,
            business_registration_days, security_index,
            tourist_arrivals_annual, latitude, longitude,
            major_towns, competitor_counts_json
        )
        SELECT 
            county_id,
            county_name,
            
            -- Population metrics with validation
            CASE 
                WHEN TRY_CAST(population_2023 AS INT) IS NULL THEN 0
                ELSE CAST(population_2023 AS INT)
            END AS population_2023,
            
            CASE 
                WHEN TRY_CAST(area_sqkm AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(area_sqkm AS DECIMAL(18,2))
            END AS area_sqkm,
            
            -- Calculated population density
            CASE 
                WHEN TRY_CAST(population_2023 AS DECIMAL(18,2)) IS NULL 
                     OR TRY_CAST(area_sqkm AS DECIMAL(18,2)) IS NULL 
                     OR CAST(area_sqkm AS DECIMAL(18,2)) = 0
                THEN 0.0
                ELSE ROUND(
                    CAST(population_2023 AS DECIMAL(18,2)) / 
                    CAST(area_sqkm AS DECIMAL(18,2)), 
                    2
                )
            END AS population_density_psqkm,
            
            -- Economic indicators with validation
            CASE 
                WHEN TRY_CAST(poverty_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(poverty_rate AS DECIMAL(5,2))
            END AS poverty_rate,
            
            CASE 
                WHEN TRY_CAST(unemployment_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(unemployment_rate AS DECIMAL(5,2))
            END AS unemployment_rate,
            
            CASE 
                WHEN TRY_CAST(avg_household_income_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(avg_household_income_kes AS DECIMAL(18,2))
            END AS avg_household_income_kes,
            
            -- Development metrics
            CASE 
                WHEN TRY_CAST(urbanization_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(urbanization_rate AS DECIMAL(5,2))
            END AS urbanization_rate,
            
            CASE 
                WHEN TRY_CAST(literacy_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(literacy_rate AS DECIMAL(5,2))
            END AS literacy_rate,
            
            -- Infrastructure scores: Keep as DECIMAL with 1 decimal place
            CASE 
                WHEN TRY_CAST(road_infrastructure_score AS DECIMAL(3,1)) IS NULL THEN 5.0
                WHEN TRY_CAST(road_infrastructure_score AS DECIMAL(3,1)) < 1.0 THEN 1.0
                WHEN TRY_CAST(road_infrastructure_score AS DECIMAL(3,1)) > 10.0 THEN 10.0
                ELSE CAST(road_infrastructure_score AS DECIMAL(3,1))
            END AS road_infrastructure_score,
            
            CASE 
                WHEN TRY_CAST(public_transport_score AS DECIMAL(3,1)) IS NULL THEN 5.0
                WHEN TRY_CAST(public_transport_score AS DECIMAL(3,1)) < 1.0 THEN 1.0
                WHEN TRY_CAST(public_transport_score AS DECIMAL(3,1)) > 10.0 THEN 10.0
                ELSE CAST(public_transport_score AS DECIMAL(3,1))
            END AS public_transport_score,
            
            -- Technology and business
            CASE 
                WHEN TRY_CAST(internet_penetration AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(internet_penetration AS DECIMAL(5,2))
            END AS internet_penetration,
            
            CASE 
                WHEN TRY_CAST(commercial_rent_kes_psqm AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(commercial_rent_kes_psqm AS DECIMAL(18,2))
            END AS commercial_rent_kes_psqm,
            
            CASE 
                WHEN TRY_CAST(business_registration_days AS INT) IS NULL THEN 0
                ELSE CAST(business_registration_days AS INT)
            END AS business_registration_days,
            
            -- Security index: Keep as DECIMAL with 1 decimal place
            CASE 
                WHEN TRY_CAST(security_index AS DECIMAL(3,1)) IS NULL THEN 5.0
                WHEN TRY_CAST(security_index AS DECIMAL(3,1)) < 1.0 THEN 1.0
                WHEN TRY_CAST(security_index AS DECIMAL(3,1)) > 10.0 THEN 10.0
                ELSE CAST(security_index AS DECIMAL(3,1))
            END AS security_index,
            
            -- Tourist arrivals (multiplied by 1000)
            CASE 
                WHEN TRY_CAST(tourist_arrivals_annual AS DECIMAL(18,2)) IS NULL THEN 0
                ELSE CAST(CAST(tourist_arrivals_annual AS DECIMAL(18,2)) * 1000 AS INT)
            END AS tourist_arrivals_annual,
            
            -- Geographic coordinates
            CASE 
                WHEN TRY_CAST(latitude AS DECIMAL(10,6)) IS NULL THEN 0.0
                ELSE CAST(latitude AS DECIMAL(10,6))
            END AS latitude,
            
            CASE 
                WHEN TRY_CAST(longitude AS DECIMAL(10,6)) IS NULL THEN 0.0
                ELSE CAST(longitude AS DECIMAL(10,6))
            END AS longitude,
            
            -- Additional information
            major_towns,
            competitor_counts_json
        FROM bronze.gis_counties_raw;
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 5. LOAD GIS LOCATIONS DATA (No dependencies)
        -- --------------------------------------------------------------------
        PRINT '5. Loading GIS Site Assessment Data...';
        
        INSERT INTO silver.gis_locations (
            location_id, county, site_name, latitude, longitude,
            visibility_score, accessibility_score, estimated_daily_traffic,
            parking_capacity, zoning, property_size_sqm, building_condition,
            competition_within_1km, complementary_businesses,
            last_survey_date, data_source
        )
        SELECT 
            -- Standardized location ID
            'LOC-' + UPPER(SUBSTRING(county, 1, 4)) + '-' + 
            SUBSTRING(location_id, 9, 10) AS location_id,
            
            county,
            site_name,
            
            -- Coordinate imputation: if NULL, use previous record + offset
            COALESCE(
                TRY_CAST(l.latitude AS DECIMAL(10,6)), 
                TRY_CAST(prev.latitude AS DECIMAL(10,6)) + 0.000034,
                0.0
            ) AS latitude,
            
            COALESCE(
                TRY_CAST(l.longitude AS DECIMAL(10,6)), 
                TRY_CAST(prev.longitude AS DECIMAL(10,6)) + 0.000134,
                0.0
            ) AS longitude,
            
            -- Assessment scores
            CASE 
                WHEN TRY_CAST(l.visibility_score AS INT) IS NULL THEN 0
                ELSE CAST(l.visibility_score AS INT)
            END AS visibility_score,
            
            CASE 
                WHEN TRY_CAST(l.accessibility_score AS INT) IS NULL THEN 0
                ELSE CAST(l.accessibility_score AS INT)
            END AS accessibility_score,
            
            -- Traffic estimation (truncate to 3 digits if >1000)
            CASE 
                WHEN TRY_CAST(l.estimated_daily_traffic AS INT) IS NULL THEN 0
                WHEN CAST(l.estimated_daily_traffic AS INT) > 1000 
                    THEN CAST(LEFT(l.estimated_daily_traffic, 3) AS INT)
                ELSE CAST(l.estimated_daily_traffic AS INT)
            END AS estimated_daily_traffic,
            
            -- Capacity metrics
            CASE 
                WHEN TRY_CAST(l.parking_capacity AS INT) IS NULL THEN 0
                ELSE CAST(l.parking_capacity AS INT)
            END AS parking_capacity,
            
            zoning,
            
            CASE 
                WHEN TRY_CAST(l.property_size_sqm AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(l.property_size_sqm AS DECIMAL(18,2))
            END AS property_size_sqm,
            
            building_condition,
            
            -- Competitive metrics
            CASE 
                WHEN TRY_CAST(l.competition_within_1km AS INT) IS NULL THEN 0
                ELSE CAST(l.competition_within_1km AS INT)
            END AS competition_within_1km,
            
            complementary_businesses,
            
            -- Survey date
            TRY_CONVERT(DATE, last_survey_date) AS last_survey_date,
            
            data_source
        FROM bronze.gis_locations_raw l
        OUTER APPLY (
            -- Find previous valid coordinates for imputation
            SELECT TOP 1 latitude, longitude
            FROM bronze.gis_locations_raw prev
            WHERE prev.county = l.county
                AND prev.location_id < l.location_id
                AND TRY_CAST(prev.latitude AS DECIMAL(10,6)) IS NOT NULL
                AND TRY_CAST(prev.longitude AS DECIMAL(10,6)) IS NOT NULL
            ORDER BY prev.location_id DESC
        ) prev;
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 6. LOAD ECONOMIC INDICATORS DATA (No dependencies)
        -- --------------------------------------------------------------------
        PRINT '6. Loading Economic Indicators Data...';
        
        WITH EconomicData AS (
            SELECT
                -- County name standardization (proper case)
                CASE 
                    WHEN county IS NOT NULL AND LTRIM(RTRIM(county)) <> ''
                    THEN UPPER(LEFT(LTRIM(RTRIM(county)), 1)) + 
                         LOWER(SUBSTRING(LTRIM(RTRIM(county)), 2, LEN(county)))
                    ELSE NULL
                END AS county,
                
                year_month,
                
                -- Year/month validation
                CASE 
                    WHEN TRY_CAST(year AS INT) IS NULL THEN 0
                    ELSE CAST(year AS INT)
                END AS year,
                
                CASE 
                    WHEN TRY_CAST(month AS INT) IS NULL THEN 0
                    ELSE CAST(month AS INT)
                END AS month,
                
                -- Calculate rolling averages for missing values
                AVG(CASE 
                    WHEN TRY_CAST(gdp_growth_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(gdp_growth_rate AS DECIMAL(5,2))
                END) OVER(
                    PARTITION BY county 
                    ORDER BY CAST(month AS INT)
                ) AS average_gdp_growth,
                
                -- GDP growth rate with validation
                CASE 
                    WHEN TRY_CAST(gdp_growth_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(gdp_growth_rate AS DECIMAL(5,2))
                END AS gdp_growth_rate,
                
                -- Inflation rate with validation and rolling average
                CASE 
                    WHEN TRY_CAST(inflation_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(inflation_rate AS DECIMAL(5,2))
                END AS inflation_rate,
                
                AVG(CASE 
                    WHEN TRY_CAST(inflation_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(inflation_rate AS DECIMAL(5,2))
                END) OVER(
                    PARTITION BY county 
                    ORDER BY CAST(month AS INT)
                ) AS average_inflation_rate,
                
                -- Unemployment rate with validation and rolling average
                CASE 
                    WHEN TRY_CAST(unemployment_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(unemployment_rate AS DECIMAL(5,2))
                END AS unemployment_rate,
                
                AVG(CASE 
                    WHEN TRY_CAST(unemployment_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(unemployment_rate AS DECIMAL(5,2))
                END) OVER(
                    PARTITION BY county 
                    ORDER BY CAST(month AS INT)
                ) AS average_unemployment_rate,
                
                -- Confidence indices
                CASE 
                    WHEN TRY_CAST(consumer_confidence_index AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(consumer_confidence_index AS DECIMAL(5,2))
                END AS consumer_confidence_index,
                
                CASE 
                    WHEN TRY_CAST(retail_sales_index AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(retail_sales_index AS DECIMAL(5,2))
                END AS retail_sales_index,
                
                CASE 
                    WHEN TRY_CAST(business_confidence_index AS DECIMAL(5,2)) IS NULL THEN 0.0
                    ELSE CAST(business_confidence_index AS DECIMAL(5,2))
            END AS business_confidence_index,
            
            -- Business registrations (negative values corrected)
            CASE 
                WHEN TRY_CAST(new_business_registrations AS INT) IS NULL THEN 0
                WHEN CAST(new_business_registrations AS INT) < 0 THEN 0
                ELSE CAST(new_business_registrations AS INT)
            END AS new_business_registrations,
            
            -- Real estate indicators
            CASE 
                WHEN TRY_CAST(commercial_rent_growth AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(commercial_rent_growth AS DECIMAL(5,2))
            END AS commercial_rent_growth,
            
            CASE 
                WHEN TRY_CAST(retail_vacancy_rate AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(retail_vacancy_rate AS DECIMAL(5,2))
            END AS retail_vacancy_rate,
            
            -- Cost indicators
            CASE 
                WHEN TRY_CAST(avg_fuel_price_kes AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(avg_fuel_price_kes AS DECIMAL(18,2))
            END AS avg_fuel_price_kes,
            
            CASE 
                WHEN TRY_CAST(usd_kes_exchange_rate AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(usd_kes_exchange_rate AS DECIMAL(18,2))
            END AS usd_kes_exchange_rate,
            
            -- Collection date
            CASE 
                WHEN TRY_CONVERT(DATE, data_collection_date) IS NOT NULL 
                    THEN TRY_CONVERT(DATE, data_collection_date)
                ELSE NULL
            END AS data_collection_date,
            
            -- Data source
            data_source AS original_data_source
        FROM bronze.economic_raw
    )
    INSERT INTO silver.economic (
        county, year_month, year, month, gdp_growth_rate,
        inflation_rate, unemployment_rate, consumer_confidence_index,
        retail_sales_index, business_confidence_index,
        new_business_registrations, commercial_rent_growth,
        retail_vacancy_rate, avg_fuel_price_kes,
        usd_kes_exchange_rate, data_collection_date, data_source
    )
    SELECT 
        county,
        year_month,
        year,
        month,
        
        -- Use rolling average if original value is NULL
        ISNULL(gdp_growth_rate, average_gdp_growth) AS gdp_growth_rate,
        ISNULL(inflation_rate, average_inflation_rate) AS inflation_rate,
        ISNULL(unemployment_rate, average_unemployment_rate) AS unemployment_rate,
        
        consumer_confidence_index,
        retail_sales_index,
        business_confidence_index,
        new_business_registrations,
        commercial_rent_growth,
        retail_vacancy_rate,
        avg_fuel_price_kes,
        usd_kes_exchange_rate,
        data_collection_date,
        original_data_source
    FROM EconomicData;
    
    SET @RowsAffected = @@ROWCOUNT;
    PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 7. LOAD COMPETITORS DATA (Parent table - must load before child)
        -- --------------------------------------------------------------------
        PRINT '7. Loading Competitor Profile Data (Parent Table)...';
        
        INSERT INTO silver.competitors (
            competitor_id, competitor_name, competitor_type,
            year_founded, total_stores, estimated_market_share,
            avg_store_revenue_kes_monthly, positioning, target_demographic,
            pricing_index, store_formats, key_strengths, key_weaknesses,
            last_updated, data_source
        )
        SELECT 
            competitor_id,
            competitor_name,
            competitor_type,
            
            -- Numeric conversions with validation
            CASE 
                WHEN TRY_CAST(year_founded AS INT) IS NULL THEN NULL
                ELSE CAST(year_founded AS INT)
            END AS year_founded,
            
            CASE 
                WHEN TRY_CAST(total_stores AS INT) IS NULL THEN 0
                ELSE CAST(total_stores AS INT)
            END AS total_stores,
            
            CASE 
                WHEN TRY_CAST(estimated_market_share AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(estimated_market_share AS DECIMAL(5,2))
            END AS estimated_market_share,
            
            CASE 
                WHEN TRY_CAST(avg_store_revenue_kes_monthly AS DECIMAL(18,2)) IS NULL THEN 0.0
                ELSE CAST(avg_store_revenue_kes_monthly AS DECIMAL(18,2))
            END AS avg_store_revenue_kes_monthly,
            
            -- Strategic information
            positioning,
            target_demographic,
            
            CASE 
                WHEN TRY_CAST(pricing_index AS DECIMAL(5,2)) IS NULL THEN 0.0
                ELSE CAST(pricing_index AS DECIMAL(5,2))
            END AS pricing_index,
            
            store_formats,
            key_strengths,
            key_weaknesses,
            
            -- Date parsing
            TRY_CONVERT(DATE, last_updated) AS last_updated,
            
            data_source
        FROM bronze.competitors_raw;
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- 8. LOAD COMPETITOR STORES DATA (Child table - depends on competitors)
        -- --------------------------------------------------------------------
        PRINT '8. Loading Competitor Store Locations Data (Child Table)...';
        PRINT '   Note: Column mapping correction applied due to CSV import issues';
        
        INSERT INTO silver.competitor_stores (
            store_id, competitor_id, county, town, store_size_sqm,
            store_format, opening_date, estimated_monthly_revenue_kes,
            estimated_daily_customers, location_score, parking_available,
            has_delivery, last_verified, data_source
        )
        SELECT 
            store_id,
            competitor_id,
            
            -- Column mapping correction: town → county (with proper casing)
            CASE 
                WHEN town IS NOT NULL AND LTRIM(RTRIM(town)) <> ''
                THEN UPPER(LEFT(LTRIM(RTRIM(town)), 1)) + 
                     LOWER(SUBSTRING(LTRIM(RTRIM(town)), 2, LEN(town)))
                ELSE NULL
            END AS county,
            
            -- Column mapping correction: store_size_sqm → town
            store_size_sqm AS town,
            
            -- Column mapping correction: store_format → store_size_sqm
            CASE 
                WHEN TRY_CONVERT(INT, store_format) IS NULL THEN 0
                WHEN TRY_CONVERT(INT, store_format) < 0 THEN 0
                ELSE TRY_CONVERT(INT, store_format)
            END AS store_size_sqm,
            
            -- Column mapping correction: opening_date → store_format
            opening_date AS store_format,
            
            -- Column mapping correction: estimated_monthly_revenue_kes → opening_date
            CASE 
                WHEN TRY_CONVERT(DATE, estimated_monthly_revenue_kes) IS NOT NULL 
                    THEN TRY_CONVERT(DATE, estimated_monthly_revenue_kes)
                ELSE NULL
            END AS opening_date,
            
            -- Column mapping correction: estimated_daily_customers → estimated_monthly_revenue_kes
            CASE 
                WHEN TRY_CONVERT(DECIMAL(18,2), estimated_daily_customers) IS NULL THEN 0.0
                WHEN TRY_CONVERT(DECIMAL(18,2), estimated_daily_customers) < 0 THEN 0.0
                ELSE CAST(estimated_daily_customers AS DECIMAL(18,2))
            END AS estimated_monthly_revenue_kes,
            
            -- Column mapping correction: location_score → estimated_daily_customers
            CASE 
                WHEN TRY_CAST(location_score AS INT) IS NULL THEN 0
                WHEN TRY_CAST(location_score AS INT) < 0 THEN 0
                ELSE CAST(location_score AS INT)
            END AS estimated_daily_customers,
            
            -- Column mapping correction: parking_available → location_score
            CASE 
                WHEN TRY_CAST(parking_available AS INT) IS NULL THEN 1
                WHEN TRY_CAST(parking_available AS INT) < 1 THEN 1
                WHEN TRY_CAST(parking_available AS INT) > 10 THEN 10
                ELSE CAST(parking_available AS INT)
            END AS location_score,
            
            -- Column mapping correction: has_delivery → parking_available
            CASE 
                WHEN UPPER(LTRIM(RTRIM(has_delivery))) IN ('YES', 'Y', '1', 'TRUE') THEN 'Yes'
                WHEN UPPER(LTRIM(RTRIM(has_delivery))) IN ('NO', 'N', '0', 'FALSE') THEN 'No'
                ELSE 'Unknown'
            END AS parking_available,
            
            -- Column mapping correction: last_verified → has_delivery
            CASE 
                WHEN UPPER(LTRIM(RTRIM(last_verified))) IN ('YES', 'Y', '1', 'TRUE') THEN 'Yes'
                WHEN UPPER(LTRIM(RTRIM(last_verified))) IN ('NO', 'N', '0', 'FALSE') THEN 'No'
                ELSE 'Unknown'
            END AS has_delivery,
            
            -- Column mapping correction: LEFT(data_source,10) → last_verified
            CASE 
                WHEN TRY_CONVERT(DATE, LEFT(data_source, 10)) IS NOT NULL 
                    THEN TRY_CONVERT(DATE, LEFT(data_source, 10))
                ELSE NULL
            END AS last_verified,
            
            -- Column mapping correction: SUBSTRING(data_source,12) → data_source
            CASE 
                WHEN LEN(data_source) > 10 
                    THEN SUBSTRING(data_source, 12, LEN(data_source))
                ELSE data_source
            END AS data_source
        FROM bronze.competitor_stores_raw
        WHERE store_id NOT LIKE '%DUP';  -- Remove duplicate markers
        
        SET @RowsAffected = @@ROWCOUNT;
        PRINT '   • Rows loaded: ' + FORMAT(@RowsAffected, 'N0');
        
        -- --------------------------------------------------------------------
        -- TRANSACTION COMMIT AND COMPLETION LOGGING
        -- --------------------------------------------------------------------
        COMMIT TRANSACTION;
        
        DECLARE @EndTime DATETIME = GETDATE();
        DECLARE @DurationSeconds INT = DATEDIFF(SECOND, @StartTime, @EndTime);
        DECLARE @DurationMinutes INT = @DurationSeconds / 60;
        DECLARE @RemainingSeconds INT = @DurationSeconds % 60;
        
        PRINT REPLICATE('=', 80);
        PRINT 'SILVER LAYER LOAD PROCESS COMPLETED SUCCESSFULLY';
        PRINT 'Completion Time: ' + FORMAT(@EndTime, 'yyyy-MM-dd HH:mm:ss');
        PRINT 'Total Duration: ' + 
              CASE WHEN @DurationMinutes > 0 
                   THEN CAST(@DurationMinutes AS NVARCHAR) + ' minutes, ' 
                   ELSE '' 
              END + 
              CAST(@RemainingSeconds AS NVARCHAR) + ' seconds';
        PRINT 'Total tables reloaded: 8';
        PRINT 'Note: silver.competitors was cleared using DELETE to maintain referential integrity';
        PRINT REPLICATE('=', 80);
        
        RETURN 0;  -- Success
        
    END TRY
    BEGIN CATCH
        -- --------------------------------------------------------------------
        -- ERROR HANDLING AND TRANSACTION ROLLBACK
        -- --------------------------------------------------------------------
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Capture error details
        SELECT 
            @ErrorMsg = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();
        
        -- Log error details
        PRINT REPLICATE('*', 80);
        PRINT 'ERROR: SILVER LAYER LOAD PROCESS FAILED';
        PRINT 'Error Time: ' + FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss');
        PRINT 'Procedure: ' + @ProcedureName;
        PRINT 'Error Message: ' + @ErrorMsg;
        PRINT 'Error Severity: ' + CAST(@ErrorSeverity AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(@ErrorState AS NVARCHAR(10));
        PRINT REPLICATE('*', 80);
        
        -- Re-throw error for external handling
        RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
        
        RETURN -1;  -- Failure
        
    END CATCH
END;
GO

/*
===============================================================================
PROCEDURE EXECUTION EXAMPLES
===============================================================================

-- Basic execution (uses current date)
EXEC silver.usp_LoadSilverLayer;

-- Execution with specific load date
EXEC silver.usp_LoadSilverLayer @LoadDate = '2025-12-19';

-- Execution with debug mode enabled
EXEC silver.usp_LoadSilverLayer @DebugMode = 1;

-- Full parameter execution
EXEC silver.usp_LoadSilverLayer 
    @LoadDate = '2025-12-19',
    @DebugMode = 1;
===============================================================================
*/

