CREATE OR REPLACE FUNCTION update_levels_longterm()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW levels_longterm;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_levels_longterm_from_ticks
AFTER INSERT ON ticks
FOR EACH ROW EXECUTE FUNCTION update_levels_longterm();