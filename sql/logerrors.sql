SET ROLE postgres;
SELECT pg_log_errors_reset();
SELECT blah();
SELECT pg_sleep(10);
SELECT * FROM pg_log_errors_stats();
DO LANGUAGE plpgsql $$
BEGIN
    RAISE SQLSTATE 'XXXXX';
END;
$$;
DO LANGUAGE plpgsql $$
BEGIN
    RAISE SQLSTATE 'XXXXY';
END;
$$;
SELECT pg_sleep(5);
SELECT * FROM pg_log_errors_stats();
