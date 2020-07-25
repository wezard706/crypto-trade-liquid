CREATE TABLE inago (
id INT NOT NULL AUTO_INCREMENT,
board_name VARCHAR(255) NOT NULL,
taker_side VARCHAR(5) NOT NULL,
volume FLOAT NOT NULL,
last_price FLOAT NOT NULL,
pair_currency VARCHAR(255) NOT NULL,
from_unix_time BIGINT NOT NULL,
to_unix_time BIGINT NOT NULL,
from_datetime DATETIME NOT NULL,
to_datetime DATETIME NOT NULL,
PRIMARY KEY (id)
)

ALTER TABLE inago ADD timestamp BIGINT;