CREATE OR REPLACE FUNCTION public.get_count( TEXT, TEXT )
RETURNS  TABLE(Year  TEXT, t_count_total  BIGINT, t_column_name  TEXT, t_count BIGINT )
LANGUAGE plpgsql
AS $BODY$
DECLARE
p_schema        TEXT := $1;
p_tabname       TEXT := $2;
v_sql_statement TEXT;

BEGIN

SELECT STRING_AGG( 'SELECT "Year",COUNT(*),''' 
       || column_name 
       || ''',' 
       || ' COUNT("' 
       || column_name 
       || '")  FROM ' 
       || table_schema 
       || '."'
       || table_name 
	   || '" GROUP BY "Year"'
         ,' UNION ALL ' ) INTO v_sql_statement
FROM   information_schema.columns 
WHERE  table_schema   = p_schema 
       AND table_name = p_tabname; 

    IF v_sql_statement IS NOT NULL THEN
     RETURN QUERY EXECUTE   v_sql_statement;
    END IF;
END
$BODY$;