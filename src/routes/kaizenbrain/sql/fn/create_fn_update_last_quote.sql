CREATE OR REPLACE FUNCTION update_last_quote()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE positions
    SET quote_last_quote = NEW.close,
    	quote_last_quote_at = NEW.dt,
        updated_at = now()
    WHERE symbol = NEW.symbol;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER update_last_quote_from_ticks_on_insert
AFTER INSERT ON ticks_1d
FOR EACH ROW EXECUTE FUNCTION update_last_quote();


CREATE TRIGGER update_last_quote_from_ticks_on_update
AFTER UPDATE ON ticks_1d
FOR EACH ROW EXECUTE FUNCTION update_last_quote();