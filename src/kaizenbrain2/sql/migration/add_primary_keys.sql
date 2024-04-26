ALTER TABLE ticks_1h ADD PRIMARY KEY (symbol,dt);
ALTER TABLE ticks_4h ADD PRIMARY KEY (symbol,dt);
ALTER TABLE ticks_1d ADD PRIMARY KEY (symbol,dt);
ALTER TABLE ticks_1w ADD PRIMARY KEY (symbol,dt);