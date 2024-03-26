CREATE OR REPLACE FUNCTION update_last_quote()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE positions
    SET quote_last_quote = NEW.close,
    		quote_last_quote_at = NEW.dt
    WHERE symbol = NEW.symbol;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_last_quote_from_ticks
AFTER INSERT ON ticks
FOR EACH ROW EXECUTE FUNCTION update_last_quote();