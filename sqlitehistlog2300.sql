
CREATE TABLE weather_history (
  sys_read_datetime datetime PRIMARY KEY,
  ws_cal_datetime datetime NOT NULL,
  temperature_in decimal(4,1) NOT NULL,
  temperature_out decimal(4,1) NOT NULL,
  dewpoint decimal(4,1) NOT NULL,
  rel_humidity_in tinyint(3) NOT NULL,
  rel_humidity_out tinyint(3) NOT NULL,
  wind_speed decimal(3,1) NOT NULL,
  wind_angle decimal(4,1) NOT NULL,
  wind_direction char(3) NOT NULL,
  wind_chill decimal(4,1) NOT NULL,
  rain_total decimal(5,1) NOT NULL,
  rel_pressure decimal(5,1) NOT NULL
);
