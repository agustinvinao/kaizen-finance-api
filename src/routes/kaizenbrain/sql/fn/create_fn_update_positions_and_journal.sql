DROP TRIGGER update_positions_and_journal_from_transactions ON transactions;
DROP FUNCTION update_positions_and_journal;

CREATE OR REPLACE FUNCTION update_positions_and_journal()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.symbol IS NOT NULL AND new.symbol != 'EUR.USD' THEN -- skip if symbol is null or if it's currency exchange
		IF NOT EXISTS (SELECT symbol FROM positions WHERE symbol=NEW.symbol) THEN
			INSERT INTO positions (symbol, updated_at, created_at, shares, quote_purchase, fee, amount, currency)
			SELECT NEW.symbol, now(), NEW.dt, NEW.shares, NEW.quote, NEW.fee, NEW.amount, NEW.currency
			WHERE NOT EXISTS (SELECT symbol FROM positions WHERE symbol=NEW.symbol);
		
		ELSIF NEW.category = 'buy' THEN -- buying
			UPDATE positions
			SET updated_at 		= now(),
				shares  		= shares + NEW.shares_left,
				amount 			= amount + NEW.amount + fee + NEW.fee,
				fee 				= fee + NEW.fee,
				quote_purchase 	= ((fee + NEW.fee) + (quote_purchase * shares) + (NEW.quote * NEW.shares)) / (shares + NEW.shares)
			WHERE symbol = NEW.symbol;
		END IF;

		IF NEW.category = 'sell' THEN -- selling
			-- raise notice 'SELL';
			DECLARE 
			   	tmp_shares INTEGER;
				position_shares INTEGER;
				position_quote INTEGER;
				position_fee INTEGER;
			  	t record;
			BEGIN
				tmp_shares := NEW.shares;
				-- create journal entries
				FOR t IN SELECT symbol, dt, quote, fee, shares_left, taxes, currency, exchange_rate
						 FROM transactions WHERE symbol = NEW.symbol AND shares_left > 0 AND category = 'buy' ORDER BY dt
			    LOOP 
			    		-- raise notice 't.shares_left > 0: %', t.shares_left > 0;
					IF t.shares_left > 0 AND tmp_shares > 0 THEN
						-- raise notice 't.shares_left >= NEW.shares: % % %', t.shares_left >= NEW.shares, NEW.shares, t.shares_left;
						IF t.shares_left >= NEW.shares THEN
							
							INSERT INTO journal (symbol, opened_at, closed_at, shares,	currency,
												 open_quote, close_quote, open_cost, close_cost,
												 profit,
												 profit_perc)
							VALUES 				(t.symbol, t.dt, NEW.dt, NEW.shares,t.currency,
												 t.quote, NEW.quote, t.fee + t.taxes, NEW.fee + NEW.taxes,
												 (NEW.shares*NEW.quote) - (NEW.shares*t.quote),
												 ((NEW.shares*NEW.quote) / (NEW.shares*t.quote)) - 1);
							
							tmp_shares := tmp_shares - NEW.shares;
							IF (t.shares_left - NEW.shares) > 0 THEN
								UPDATE transactions SET shares_left = (t.shares_left - NEW.shares) WHERE symbol = t.symbol AND dt = t.dt;
							ELSE
								UPDATE transactions SET shares_left = 0 						   WHERE symbol = t.symbol AND dt = t.dt;
							END IF;
							-- raise notice 't.shares_left - NEW.shares: % | tmp_shares: %', t.shares_left - NEW.shares, tmp_shares;
						END IF;
					END IF;
			    END LOOP;

				SELECT SUM(shares_left) FROM transactions WHERE symbol=NEW.symbol AND category='buy' INTO position_shares;
				IF position_shares > 0 THEN
					SELECT (sum(quote*shares_left) / sum(shares_left)), sum(fee)
					FROM transactions WHERE symbol=NEW.symbol AND category='buy'
					INTO position_quote, position_fee;

					UPDATE positions
					SET updated_at 		= now(),
						shares			= position_shares,
						quote_purchase 	= position_quote,
						fee 				= position_fee,
						amount 			= position_quote * position_shares
					WHERE symbol = NEW.symbol;
				ELSE -- delete position if all is sold
					DELETE FROM positions WHERE symbol=NEW.symbol;
				END IF;		
			END;
		END IF;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_positions_and_journal_from_transactions
AFTER INSERT ON transactions
FOR EACH ROW EXECUTE FUNCTION update_positions_and_journal();