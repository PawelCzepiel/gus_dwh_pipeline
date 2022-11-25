class SqlQueries:
    
    dwh_tables_create = (""" 
        CREATE TABLE IF NOT EXISTS public.staging_dict_areas (
            index bigint,
            id bigint NOT NULL,
            nazwa varchar(256),
            id_nadrzedny_element float8,
            id_poziom bigint,
            nazwa_poziom varchar(256),
            czy_zmienne boolean,
            PRIMARY KEY (id)
            );

        CREATE TABLE IF NOT EXISTS public.staging_dict_area_variables (
            index bigint,
            id bigint NOT NULL,
            nazwa varchar(256),
            id_zmienna bigint NOT NULL,
            nazwa_zmienna varchar(max)
            );
         
        CREATE TABLE IF NOT EXISTS public.staging_dict_variables (
            index bigint,
            id_zmienna bigint NOT NULL,
            nazwa_zmienna varchar(max),
            id_przekroj bigint,
            nazwa_przekroj varchar(max),
            id_okres bigint
            );           
            
        CREATE TABLE IF NOT EXISTS public.staging_dict_dims_sections (
            index bigint,
            id_przekroj bigint NOT NULL,
            id_wymiar bigint NOT NULL,
            nazwa_wymiar varchar(256),
            id_pozycja bigint,
            nazwa_pozycja varchar(256)
            );            

        CREATE TABLE IF NOT EXISTS public.staging_dict_pres_methods (
            index bigint,
            id_sposob_prezentacji_miara bigint NOT NULL,
            nazwa varchar(max),
            nazwa_sposob_prezentacji varchar(max),
            id_jednostka_miary bigint,
            oznaczenie_jednostki varchar(256),
            nazwa_jednostki varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_dict_periods (
            index bigint,
            id_okres bigint NOT NULL,
            symbol varchar(256),
            opis varchar(256),
            id_czestotliwosc bigint,
            nazwa_czestotliwosc varchar(256),
            id_typ bigint,
            nazwa_typ varchar(256),
            PRIMARY KEY (id_okres)
            );
            
        CREATE TABLE IF NOT EXISTS public.staging_dict_dates (
            index bigint,
            id_rok bigint NOT NULL,
            PRIMARY KEY (id_rok)
            );
            
       CREATE TABLE IF NOT EXISTS public.staging_dict_novalue_reason (
            index bigint,
            id_brak_wartosci bigint NOT NULL,
            oznaczenie varchar(256),
            nazwa varchar(max)
            );       

       CREATE TABLE IF NOT EXISTS public.staging_dict_confidentiality_code (
            index bigint,
            id_tajnosc bigint NOT NULL,
            oznaczenie varchar(256),
            nazwa varchar(256)
            ); 

       CREATE TABLE IF NOT EXISTS public.staging_dict_flags (
            index bigint,
            id_flaga bigint NOT NULL,
            oznaczenie varchar(256),
            nazwa varchar(256)
            ); 

        CREATE TABLE IF NOT EXISTS public.staging_sections (
            index bigint,
            id_przekroj bigint,
            nazwa_przekroj varchar(256),
            szereg_czasowy varchar(256),
            id_czestotliwosc bigint,
            nazwa_czestotliwosc varchar(256),
            aktualizacja_ostatnia varchar(256),
            aktualizacja_kolejna varchar(256)
            );
          
        CREATE TABLE IF NOT EXISTS public.staging_terms_307 (
            index bigint,
            id_pojecie bigint NOT NULL,
            nazwa_pojecie varchar(max),
            definicja varchar(max),
            uwagi varchar(max),
            data_poczatku_pojecie varchar(256),
            data_konca_pojecie varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_terms_400 (
            index bigint,
            id_pojecie bigint NOT NULL,
            nazwa_pojecie varchar(max),
            definicja varchar(max),
            uwagi varchar(max),
            data_poczatku_pojecie varchar(256),
            data_konca_pojecie varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_terms_303 (
            index bigint,
            id_pojecie bigint NOT NULL,
            nazwa_pojecie varchar(max),
            definicja varchar(max),
            uwagi varchar(max),
            data_poczatku_pojecie varchar(256),
            data_konca_pojecie varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_departments_307 (
            index bigint,
            id_jednostka bigint NOT NULL,
            nazwa_jednostki varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_departments_400 (
            index bigint,
            id_jednostka bigint NOT NULL,
            nazwa_jednostki varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_departments_303 (
            index bigint,
            id_jednostka bigint NOT NULL,
            nazwa_jednostki varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.staging_methodology_307 (
            index bigint,
            notki varchar(max)       
            ); 

        CREATE TABLE IF NOT EXISTS public.staging_methodology_400 (
            index bigint,
            wyjasnienia_metodologiczne_tresc varchar(max),
            notki varchar(max)     
            ); 

        CREATE TABLE IF NOT EXISTS public.staging_methodology_303 (
            index bigint,
            notki varchar(max)       
            ); 

        CREATE TABLE IF NOT EXISTS public.staging_sets_307 (
            index bigint,
            id_zestaw bigint NOT NULL,
            nazwa_zestaw varchar(max),
            id_gestor bigint,
            nazwa_gestor varchar(256)           
            );

        CREATE TABLE IF NOT EXISTS public.staging_sets_400 (
            index bigint,
            id_zestaw bigint NOT NULL,
            symbol_zestaw varchar(256),
            nazwa_zestaw varchar(max),
            id_gestor bigint,
            nazwa_gestor varchar(256)           
            );

        CREATE TABLE IF NOT EXISTS public.staging_sets_303 (
            index bigint,
            id_zestaw bigint NOT NULL,
            symbol_zestaw varchar(256),
            nazwa_zestaw varchar(max),
            id_gestor bigint,
            nazwa_gestor varchar(256)           
            );
            
        CREATE TABLE IF NOT EXISTS public.staging_studies_307 (
            index bigint,
            id_badanie bigint NOT NULL,
            symbol_badanie varchar(256),
            temat_badanie varchar(max)           
            );

        CREATE TABLE IF NOT EXISTS public.staging_studies_400 (
            index bigint,
            id_badanie bigint NOT NULL,
            symbol_badanie varchar(256),
            temat_badanie varchar(max)           
            );

        CREATE TABLE IF NOT EXISTS public.staging_studies_303 (
            index bigint,
            id_badanie bigint NOT NULL,
            symbol_badanie varchar(256),
            temat_badanie varchar(256)           
            );
          
        CREATE TABLE IF NOT EXISTS public.staging_tags_307 (
            index bigint,
            id_tag bigint NOT NULL,
            nazwa_tag varchar(256)               
            );

        CREATE TABLE IF NOT EXISTS public.staging_tags_400 (
            index bigint,
            id_tag bigint NOT NULL,
            nazwa_tag varchar(256)               
            );
            
        CREATE TABLE IF NOT EXISTS public.staging_tags_303 (
            index bigint,
            id_tag bigint NOT NULL,
            nazwa_tag varchar(256)               
            );

        CREATE TABLE IF NOT EXISTS public.staging_wages (
            index bigint,
            rownumber bigint,
            id_zmienna bigint,
            id_przekroj bigint,
            id_wymiar_1 bigint,
            id_pozycja_1 bigint,
            id_okres bigint,
            id_sposob_prezentacji_miara bigint,
            id_daty bigint,
            id_brak_wartosci bigint,
            id_tajnosci bigint,
            id_flaga bigint,
            wartosc float8,
            precyzja bigint       
            );

        CREATE TABLE IF NOT EXISTS public.staging_retail_prices (
            index bigint,
            rownumber bigint,
            id_zmienna bigint,
            id_przekroj bigint,
            id_wymiar_1 bigint,
            id_pozycja_1 bigint,
            id_wymiar_2 bigint,
            id_pozycja_2 bigint,
            id_okres bigint,
            id_sposob_prezentacji_miara bigint,
            id_daty bigint,
            id_brak_wartosci bigint,
            id_tajnosci bigint,
            id_flaga bigint,
            wartosc float8,
            precyzja bigint       
            );          

        CREATE TABLE IF NOT EXISTS public.staging_realestate_prices (
            index bigint,
            rownumber bigint,
            id_zmienna bigint,
            id_przekroj bigint,
            id_wymiar_1 bigint,
            id_pozycja_1 bigint,
            id_wymiar_2 bigint,
            id_pozycja_2 bigint,
            id_okres bigint,
            id_sposob_prezentacji_miara bigint,
            id_daty bigint,
            id_brak_wartosci bigint,
            id_tajnosci bigint,
            id_flaga bigint,
            wartosc float8,
            precyzja bigint       
            );

        CREATE TABLE IF NOT EXISTS public.areas (
            area_key bigint NOT NULL,
            area_name varchar(256),
            leaditem_id float8,
            level_id bigint,
            level_name varchar(256),
            variables boolean
            );

        CREATE TABLE IF NOT EXISTS public.variables (
            areavar_key bigint identity(0,1),
            variable_id bigint NOT NULL,
            variable_name varchar(max),           
            area_id bigint NOT NULL
            );

        CREATE TABLE IF NOT EXISTS public.sections_periods (
            secper_key bigint identity(0,1),
            section_id bigint,
            section_name varchar(max),
            period_id bigint,
            variable_id bigint NOT NULL,
            CONSTRAINT secper_pkey PRIMARY KEY (section_id,period_id)
            );

        CREATE TABLE IF NOT EXISTS public.dims_positions (
            dimpos_key bigint identity(0,1),
            dim_id bigint NOT NULL,
            dim_name varchar(256),
            position_id bigint,
            position_name varchar(256),
            section_id bigint NOT NULL,
            CONSTRAINT dimpos_pkey PRIMARY KEY (dim_id,position_id)
            );

        CREATE TABLE IF NOT EXISTS public.pres_method (
            presmethod_key bigint identity(0,1),
            presmethodunit_id bigint NOT NULL,
            presmethodunit_name varchar(max),
            presmethod_name varchar(max),
            unit_id bigint,
            unit_description varchar(256),
            unit_name varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.periods (
            period_key bigint NOT NULL,
            symbol varchar(256),
            description varchar(256),
            frequency_id bigint,
            frequency_name varchar(256),
            type_id bigint,
            type_name varchar(256),
            PRIMARY KEY (period_key)
            );

        CREATE TABLE IF NOT EXISTS public.dates (
            year_key bigint NOT NULL,
            PRIMARY KEY (year_key)
            );

        CREATE TABLE IF NOT EXISTS public.novalue_reason (
            novalue_key bigint identity(0,1),
            novalue_id bigint NOT NULL,
            mark varchar(256),
            name varchar(max)
            );

        CREATE TABLE IF NOT EXISTS public.confidentiality_code (
            confidentiality_key bigint identity(0,1),
            confidentiality_id bigint NOT NULL,
            mark varchar(256),
            name varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.flags (
            flag_key bigint identity(0,1),
            flag_id bigint NOT NULL,
            mark varchar(256),
            name varchar(256)
            );

        CREATE TABLE IF NOT EXISTS public.sections (
            section_key bigint identity(0,1),
            section_id bigint,
            section_name varchar(256),
            timerange varchar(256),
            frequency_id bigint,
            frequency_name varchar(256),
            update_last varchar(256),
            update_next varchar(256)
            );  

        CREATE TABLE IF NOT EXISTS public.terms (
            term_key bigint identity(0,1),
            term_id bigint NOT NULL,
            term_name varchar(max),
            definition varchar(max),
            remarks varchar(max),
            date_start varchar(256),
            date_end varchar(256),
            variable_id bigint
            );

        CREATE TABLE IF NOT EXISTS public.departments (
            department_key bigint identity(0,1),
            department_id bigint NOT NULL,
            department_name varchar(256),
            variable_id bigint
            );

        CREATE TABLE IF NOT EXISTS public.methodology (
            method_key bigint identity(0,1),
            method_definition varchar(max),
            variable_id bigint   
            );   

        CREATE TABLE IF NOT EXISTS public.sets (
            set_key bigint identity(0,1),
            set_id bigint NOT NULL,
            set_symbol varchar(256),
            set_name varchar(max),
            admin_id bigint,
            admin_name varchar(256),
            variable_id bigint           
            );

        CREATE TABLE IF NOT EXISTS public.studies (
            study_key bigint identity(0,1),
            study_id bigint NOT NULL,
            study_symbol varchar(256),
            study_subject varchar(max),
            variable_id bigint        
            );

        CREATE TABLE IF NOT EXISTS public.tags (
            tag_key bigint identity(0,1),
            tag_id bigint NOT NULL,
            tag_name varchar(256),
            variable_id bigint               
            );

        CREATE TABLE IF NOT EXISTS public.logs (
            log_key bigint identity(0,1),
            variable_id bigint,
            section_id bigint,
            dim1_id bigint,
            position1_id bigint,
            dim2_id bigint,
            position2_id bigint,
            period_id bigint,
            presmethodunit_id bigint,
            year_id bigint,
            novalue_id bigint,
            confidentiality_id bigint,
            flag_id bigint,
            value float8,
            precision bigint       
            );

            """)
    
    areas_table_insert = ("""
        INSERT INTO areas (area_key, area_name, leaditem_id, level_id, level_name, variables)
        SELECT id, nazwa, id_nadrzedny_element, id_poziom, nazwa_poziom, czy_zmienne
        FROM staging_dict_areas
        """)
        
    variables_table_insert = ("""
        INSERT INTO variables (variable_id, variable_name, area_id)
        SELECT id_zmienna, nazwa_zmienna, id
        FROM staging_dict_area_variables
        """)
    
    sections_periods_table_insert = ("""
        INSERT INTO sections_periods (section_id, section_name, period_id, variable_id)
        SELECT id_przekroj, nazwa_przekroj, id_okres, id_zmienna
        FROM staging_dict_variables
        """)
    
    dims_positions_table_insert = ("""
        INSERT INTO dims_positions (dim_id, dim_name, position_id, position_name, section_id)
        SELECT id_wymiar, nazwa_wymiar, id_pozycja, nazwa_pozycja, id_przekroj
        FROM staging_dict_dims_sections
        """)
    
    pres_method_table_insert = ("""
        INSERT INTO pres_method (presmethodunit_id, presmethodunit_name, presmethod_name, unit_id, unit_description, unit_name)
        SELECT id_sposob_prezentacji_miara, nazwa, nazwa_sposob_prezentacji, id_jednostka_miary, oznaczenie_jednostki, nazwa_jednostki
        FROM staging_dict_pres_methods
        """)
    
    periods_table_insert = ("""
        INSERT INTO periods (period_key, symbol, description, frequency_id, frequency_name, type_id, type_name)
        SELECT id_okres, symbol, opis, id_czestotliwosc, nazwa_czestotliwosc, id_typ, nazwa_typ
        FROM staging_dict_periods
        """)
    
    dates_table_insert = ("""
        INSERT INTO dates (year_key)
        SELECT id_rok
        FROM staging_dict_dates
        """)
        
    novalue_reason_table_insert = ("""
        INSERT INTO novalue_reason (novalue_id, mark, name)
        SELECT id_brak_wartosci, oznaczenie, nazwa
        FROM staging_dict_novalue_reason
        """)
    
    confidentiality_code_table_insert = ("""
        INSERT INTO confidentiality_code (confidentiality_id, mark, name)
        SELECT id_tajnosc, oznaczenie, nazwa
        FROM staging_dict_confidentiality_code
        """)

    flags_table_insert = ("""
        INSERT INTO flags (flag_id, mark, name)
        SELECT id_flaga, oznaczenie, nazwa
        FROM staging_dict_flags
        """)  
    
    sections_table_insert = ("""
        INSERT INTO sections (section_id, section_name, timerange, frequency_id, frequency_name, update_last, update_next)
        SELECT id_przekroj, nazwa_przekroj, szereg_czasowy, id_czestotliwosc, nazwa_czestotliwosc, aktualizacja_ostatnia, aktualizacja_kolejna
        FROM staging_sections
        """)
    
    terms307_table_insert = ("""
        INSERT INTO terms (term_id, term_name, definition, remarks, date_start, date_end)
        SELECT id_pojecie, nazwa_pojecie, definicja, uwagi, data_poczatku_pojecie, data_konca_pojecie
        FROM staging_terms_307;
        UPDATE terms
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    terms400_table_insert = ("""
        INSERT INTO terms (term_id, term_name, definition, remarks, date_start, date_end)
        SELECT id_pojecie, nazwa_pojecie, definicja, uwagi, data_poczatku_pojecie, data_konca_pojecie
        FROM staging_terms_400;
        UPDATE terms
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
        
    terms303_table_insert = ("""
        INSERT INTO terms (term_id, term_name, definition, remarks, date_start, date_end)
        SELECT id_pojecie, nazwa_pojecie, definicja, uwagi, data_poczatku_pojecie, data_konca_pojecie
        FROM staging_terms_400;
        UPDATE terms
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    departments307_table_insert = ("""
        INSERT INTO departments (department_id, department_name)
        SELECT id_jednostka, nazwa_jednostki
        FROM staging_departments_307;
        UPDATE departments
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    departments400_table_insert = ("""
        INSERT INTO departments (department_id, department_name)
        SELECT id_jednostka, nazwa_jednostki
        FROM staging_departments_400;
        UPDATE departments
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
    
    departments303_table_insert = ("""
        INSERT INTO departments (department_id, department_name)
        SELECT id_jednostka, nazwa_jednostki
        FROM staging_departments_303;
        UPDATE departments
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    methodology307_table_insert = ("""
        INSERT INTO methodology (method_definition)
        SELECT notki
        FROM staging_methodology_307;
        UPDATE methodology
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    methodology400_table_insert = ("""
        INSERT INTO methodology (method_definition)
        SELECT wyjasnienia_metodologiczne_tresc
        FROM staging_methodology_400;   
        UPDATE methodology
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
    
    methodology303_table_insert = ("""
        INSERT INTO methodology (method_definition)
        SELECT notki
        FROM staging_methodology_303;
        UPDATE methodology
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    sets307_table_insert = ("""
        INSERT INTO sets (set_id, set_name, admin_id, admin_name)
        SELECT id_zestaw, nazwa_zestaw, id_gestor, nazwa_gestor
        FROM staging_sets_307;
        UPDATE sets
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    sets400_table_insert = ("""
        INSERT INTO sets (set_id, set_symbol, set_name, admin_id, admin_name)
        SELECT id_zestaw, symbol_zestaw, nazwa_zestaw, id_gestor, nazwa_gestor
        FROM staging_sets_400;
        UPDATE sets
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
    
    sets303_table_insert = ("""
        INSERT INTO sets (set_id, set_symbol, set_name, admin_id, admin_name)
        SELECT id_zestaw, symbol_zestaw, nazwa_zestaw, id_gestor, nazwa_gestor
        FROM staging_sets_303;
        UPDATE sets
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    studies307_table_insert = ("""
        INSERT INTO studies (study_id, study_symbol, study_subject)
        SELECT id_badanie, symbol_badanie, temat_badanie
        FROM staging_studies_307;
        UPDATE studies
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    studies400_table_insert = ("""
        INSERT INTO studies (study_id, study_symbol, study_subject)
        SELECT id_badanie, symbol_badanie, temat_badanie
        FROM staging_studies_400;
        UPDATE studies
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
    
    studies303_table_insert = ("""
        INSERT INTO studies (study_id, study_symbol, study_subject)
        SELECT id_badanie, symbol_badanie, temat_badanie
        FROM staging_studies_303;
        UPDATE studies
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    tags307_table_insert = ("""
        INSERT INTO tags (tag_id, tag_name)
        SELECT id_tag, nazwa_tag
        FROM staging_tags_307;
        UPDATE tags
        SET variable_id = 307
        WHERE variable_id IS NULL
        """)
    
    tags400_table_insert = ("""
        INSERT INTO tags (tag_id, tag_name)
        SELECT id_tag, nazwa_tag
        FROM staging_tags_400;
        UPDATE tags
        SET variable_id = 400
        WHERE variable_id IS NULL
        """)
    
    tags303_table_insert = ("""
        INSERT INTO tags (tag_id, tag_name)
        SELECT id_tag, nazwa_tag
        FROM staging_tags_303;
        UPDATE tags
        SET variable_id = 303
        WHERE variable_id IS NULL
        """)
    
    logs_wages_table_insert = ("""
        INSERT INTO logs (variable_id, section_id, dim1_id, position1_id,
                           period_id, presmethodunit_id, year_id, novalue_id, confidentiality_id,
                           flag_id, value, precision )
        SELECT id_zmienna, id_przekroj, id_wymiar_1, id_pozycja_1,
                id_okres, id_sposob_prezentacji_miara, id_daty, id_brak_wartosci, id_tajnosci,
                id_flaga, wartosc, precyzja
        FROM staging_wages
        """)
    
    logs_retail_prices_table_insert = ("""
        INSERT INTO logs (variable_id, section_id, dim1_id, position1_id, dim2_id, position2_id,
                           period_id, presmethodunit_id, year_id, novalue_id, confidentiality_id,
                           flag_id, value, precision )
        SELECT id_zmienna, id_przekroj, id_wymiar_1, id_pozycja_1, id_wymiar_2, id_pozycja_2,
                id_okres, id_sposob_prezentacji_miara, id_daty, id_brak_wartosci, id_tajnosci,
                id_flaga, wartosc, precyzja
        FROM staging_retail_prices 
        """)
    
    logs_realestate_table_insert = ("""
        INSERT INTO logs (variable_id, section_id, dim1_id, position1_id, dim2_id, position2_id,
                           period_id, presmethodunit_id, year_id, novalue_id, confidentiality_id,
                           flag_id, value, precision )
        SELECT id_zmienna, id_przekroj, id_wymiar_1, id_pozycja_1, id_wymiar_2, id_pozycja_2,
                id_okres, id_sposob_prezentacji_miara, id_daty, id_brak_wartosci, id_tajnosci,
                id_flaga, wartosc, precyzja
        FROM staging_realestate_prices  
        """)