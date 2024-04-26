# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.15] - 2024-04-19
### Added
- moved code to classes

## [0.0.14] - 2024-04-15
### Added
- timezone fix to UTC values

## [0.0.12] - 2024-04-09
### Added
- assets without update on time

## [0.0.11] - 2024-04-09
### Added
- class variables for db and mqtt
- debug flag

## [0.0.10] - 2024-04-08
### Fixed
- Adding 1h internval

## [0.0.9] - 2024-04-08
### Fixed
- InsiderTrading run methods, now fetchs insider_details

## [0.0.8] - 2024-04-08
### Fixed
- InsiderTrading class code refactored and insert errors fixed

## [0.0.7] - 2024-04-07
### Added
- InsiderTrading class for fetching data from sec.gov
```python
# config must include DB (HOST,USER,PASSWORD,DATABASE) and
# MQTT(MQTT_PASSWORD,MQTT_USER,MQTT_HOST) environment variables.

import kaizenbrain
kaizenbrain.update_insider_trading(config)
```



