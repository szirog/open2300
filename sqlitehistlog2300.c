/*  
 *  open2300 - sqlitehistlog2300.c
 *  
 *  Open2300 1.11  - sqlitehistlog2300 1.0
 *
 *  Get history data fromn WS2300 weather station
 *  and the unstored records store in an SQLite database
 *
 *  This program is published uhder the GNU General Public License 
 *
 * Copyright 2013, SziroG
*/ 

/* The first initial source file headers below */ 
/* open2300 - histlog2300.c
 *  
 *  Version 1.15 (open200 1.11) Lars Hinrichsen
 *  
 *  Control WS2300 weather station
 *  
 *  Copyright 2003-2005, Kenneth Lavrsen
 *  This program is published under the GNU General Public license
 * 
 *  History:
 *  Version 1.12 
 *  - Code refactored
 * 
 *  1,14 2006  July 19 (included in open2300 1.11)
 *  1.15 2007  July 19  EmilianoParasassi
 *             http://www.lavrsen.dk/twiki/bin/view/Open2300/MysqlPatch2 
 */

/* The second used source file headers below */ 
/*		 sqlitelog2300.c
 *
 *		 Open2300 1.11 - sqlitelog2300 1.0
 *
 *		 Get current data from WS2300 weather station
 *		 and add store it in an SQLite database
 *
 *		 Copyright 2010, Wesley Moore
 *
 *		 This program is published under the GNU General Public license
 *
 */



#include <sqlite3.h>
#include "rw2300.h"


/********************************************************************
 * print_usage prints a short user guide
 *
 * Input:   none
 * 
 * Output:  prints to stdout
 * 
 * Returns: exits program
 *
 ********************************************************************/
void print_usage(void)
{
	printf("\n");
	printf("sqlitehistlog2300 - Log history data from WS-2300 to SQLite.\n");
	printf("Version %s (C)2013 SziroG\n", VERSION);
	printf("This program is released under the GNU General Public License (GPL)\n\n");
	printf("Usage:\n");
	printf("sqlitehistlog2300 sqlite_db_filename [config_filename]\n");
	exit(0);
}

/* Imported from the modified sqlitelog2300.c source */
/********************************************************************
 * The state struct stores the state of this program. It exists to
 * allow clean shutdown in the event an error ocurrs.
 *
 ********************************************************************/
struct state {
	sqlite3 *db;
//	WEATHERSTATION station;
	sqlite3_stmt *statement;
};

/* QUERY_BUF_SIZE specifies the size in bytes for the SQL INSERT statement
 * that is built */
#define QUERY_BUF_SIZE 4096


/* Imported from the modified sqlitelog2300.c source */
/********************************************************************
 * state_init initializes a state struct
 *
 * Input:	Pointer to state structure to initialize
 *			An open2300 configuration
 *			The path to the SQLite database
 *			A NULL terminated array of column names in the weather
 *			table.
 *
 * Output:	Initialized state structure
 *
 * Returns: void
 *
 ********************************************************************/
void state_init(struct state* state, const char *db_path, char *query)
{
	int i, rc;
	//char query[QUERY_BUF_SIZE + 1] = ""; /* +1 for trailing NUL */

	/* Connect to the weather station */
//	state->station = open_weatherstation(config->serial_device_name);

	/* Connect to the database */
	rc = sqlite3_open(db_path, &state->db);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to open database (%s): %s\n", db_path, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}

	/* Ensure there's always a NUL char at the end of the buffer.
	   strncat won't override this char as its beyond QUERY_BUF_SIZE */
/*
	query[QUERY_BUF_SIZE] = '\0';
	strncat(query, "INSERT INTO weather_history (", QUERY_BUF_SIZE);
	for(i = 0; column_names[i] != NULL; i++) {
		  strncat(query, column_names[i], QUERY_BUF_SIZE);
		  if(column_names[i + 1] != NULL) strncat(query, ", ", QUERY_BUF_SIZE);
	}
	strncat(query, ") VALUES (", QUERY_BUF_SIZE);
	for(i = 0; column_names[i] != NULL; i++) {
		/*	Commented out by SziroG  
		if(strcmp(column_names[i], "datetime") == 0)
		{
		}
		else
		{
		*/
/*
		strncat(query, ":", QUERY_BUF_SIZE);
		strncat(query, column_names[i], QUERY_BUF_SIZE);
		/*	Commented out by SziroG  
		}
		*/
/*
		if(column_names[i + 1] != NULL) strncat(query, ", ", QUERY_BUF_SIZE);
	}
	strncat(query, ")", QUERY_BUF_SIZE);


	/* Prepare the query */
	int nByte = -1;
	rc = sqlite3_prepare_v2(state->db, query, nByte, &state->statement, NULL);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
}

void state_reinit(struct state* state, char * const *column_names)
{
	int i, rc;
	char query[QUERY_BUF_SIZE + 1] = ""; /* +1 for trailing NUL */

	rc = sqlite3_step(state->statement);
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Unable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
	rc = sqlite3_finalize(state->statement);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Unable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}

	/* Ensure there's always a NUL char at the end of the buffer.
	   strncat won't override this char as its beyond QUERY_BUF_SIZE */
	query[QUERY_BUF_SIZE] = '\0';
	strncat(query, "INSERT INTO weather_history (", QUERY_BUF_SIZE);
	for(i = 0; column_names[i] != NULL; i++) {
		  strncat(query, column_names[i], QUERY_BUF_SIZE);
		  if(column_names[i + 1] != NULL) strncat(query, ", ", QUERY_BUF_SIZE);
	}
	strncat(query, ") VALUES (", QUERY_BUF_SIZE);
	for(i = 0; column_names[i] != NULL; i++) {
		/*	Commented out by SziroG  
		if(strcmp(column_names[i], "datetime") == 0)
		{
		}
		else
		{
		*/
		strncat(query, ":", QUERY_BUF_SIZE);
		strncat(query, column_names[i], QUERY_BUF_SIZE);
		/*	Commented out by SziroG  
		}
		*/
		if(column_names[i + 1] != NULL) strncat(query, ", ", QUERY_BUF_SIZE);
	}
	strncat(query, ")", QUERY_BUF_SIZE);


	/* Prepare the query */
	int nByte = -1;
	rc = sqlite3_prepare_v2(state->db, query, nByte, &state->statement, NULL);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
}


void state_init2(struct state* state, const char *db_path)
{
	int rc;
	char query[QUERY_BUF_SIZE + 1] = ""; /* +1 for trailing NUL */

	/* Connect to the weather station */
//	state->station = open_weatherstation(config->serial_device_name);

	/* Connect to the database */
	rc = sqlite3_open(db_path, &state->db);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to open database (%s): %s\n", db_path, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}

	/* Ensure there's always a NUL char at the end of the buffer.
	   strncat won't override this char as its beyond QUERY_BUF_SIZE */
	query[QUERY_BUF_SIZE] = '\0';

        /* Query for selecting max date from weather_history */
	strncat(query, "select max(ws_cal_datetime) from weather_history", QUERY_BUF_SIZE);

	/* Prepare the query */
	int nByte = -1;
	rc = sqlite3_prepare_v2(state->db, query, nByte, &state->statement, NULL);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
	
}

/* Imported from the modified sqlitelog2300.c source */
/********************************************************************
 * state_finish finalizes a state struct. The struct is not usable
 * after this function has been called on it. This function should
 * be called to free resources when the state is no longer needed.
 *
 * Input:	Pointer to state structure to finalize
 *
 * Output:	Finalized state structure
 *
 * Returns: void
 *
 ********************************************************************/
void state_finish(struct state* state)
{
	sqlite3_finalize(state->statement);
	sqlite3_close(state->db);
//	close_weatherstation(state->station);
}

/* Imported from the modified sqlitelog2300.c source */
/********************************************************************
 * check_rc ensures the passed return code (rc) from SQLite
 * indicates success. If not it prints the current SQLite
 * error message, finalizes the state and exits the program.
 *
 * Input:	Pointer to state structure to finalize
 *			Return code from SQLite function
 *
 * Output:	Program exit if the return code indicates an error
 *
 * Returns: void
 *
 ********************************************************************/
void check_rc(struct state* state, int rc)
{
	if(rc != SQLITE_OK) {
		fprintf(stderr, "Unable to bind value: %s\n", sqlite3_errmsg(state->db));
		state_finish(state);
		exit(EXIT_FAILURE);
	}
}




/********** MAIN PROGRAM ************************************************
 *
 * This program reads the history records from a WS2300
 * weather station at a given record range
 * and prints the data to stdout and to a file.
 * Just run the program without parameters for usage.
 *
 * It uses the config file for device name.
 * Config file locations - see open2300.conf-dist
 *
 ***********************************************************************/
int main(int argc, char *argv[])
{
	WEATHERSTATION ws2300;
/*
	MYSQL mysql, *mysql_connection;
	int mysql_state;
	char mysql_insert_stmt[512] = 
		"INSERT INTO weather(datetime, temp_in, temp_out, dewpoint, rel_hum_in, "
		"rel_hum_out, wind_speed, wind_angle, wind_direction,wind_chill, rain_total, rel_pressure)";
	char mysql_values_stmt[512], mysql_stmt[1024];
*/
	int interval, countdown, no_records;
	struct config_type config;
	char datestring[50];        //used to hold the date stamp for the log file
	struct timestamp time_last;
	time_t time_lastlog, time_lastrecord;
	struct tm time_lastlog_tm, time_lastrecord_tm;
	int current_record, next_record, lastlog_record, new_records;
	double temperature_in;
	double temperature_out;
	double dewpoint;
	double windchill;
	double pressure;
	double pressure_term;
	int humidity_in;
	int humidity_out;
	double rain;
	double windspeed;
	double winddir_degrees;
	const char *directions[]= {"N","NNE","NE","ENE","E","ESE","SE","SSE",
	                           "S","SSW","SW","WSW","W","WNW","NW","NNW"};
	int i;

	
/*	
	double winddir[6];
	int winddir_index;
*/
	char *columns[] = {
		"sys_read_datetime",
		"ws_cal_datetime",
		"temperature_in",
		"temperature_out",
		"dewpoint",
		"rel_humidity_in",
		"rel_humidity_out",
		"wind_speed",
		"wind_angle",
		"wind_direction",
		"wind_chill",
		"rain_total",
		"rel_pressure",
		NULL
	};

	int rc;
	time_t rt;
	char rtstring[50];
	const char select_stmt = "SELECT MAX(ws_cal_datetime) FROM weather_history";
	char insert_stmt[QUERY_BUF_SIZE + 1] = ""; /* +1 for trailing NUL */

	/* Read the configuration */
	if(argc >= 3) {
		get_configuration(&config, argv[2]);
	}
	else if(argc == 2) {
		get_configuration(&config, NULL);
	}
	else {
		print_usage();
		exit(2);
	}

	/* Ensure there's always a NUL char at the end of the buffer.
	   strncat won't override this char as its beyond QUERY_BUF_SIZE */
	insert_stmt[QUERY_BUF_SIZE] = '\0';
	strncat(insert_stmt, "INSERT INTO weather_history (", QUERY_BUF_SIZE);
	for(i = 0; columns[i] != NULL; i++) {
		  strncat(insert_stmt, columns[i], QUERY_BUF_SIZE);
		  if(columns[i + 1] != NULL) strncat(insert_stmt, ", ", QUERY_BUF_SIZE);
	}
	strncat(insert_stmt, ") VALUES (", QUERY_BUF_SIZE);
	for(i = 0; columns[i] != NULL; i++) {
		strncat(insert_stmt, ":", QUERY_BUF_SIZE);
		strncat(insert_stmt, columns[i], QUERY_BUF_SIZE);
		if(columns[i + 1] != NULL) strncat(insert_stmt, ", ", QUERY_BUF_SIZE);
	}
	strncat(insert_stmt, ")", QUERY_BUF_SIZE);
	

	// Setup serial port
	ws2300 = open_weatherstation(config.serial_device_name);

	// Open SQLite database file for new record(s) inserting
	struct state s;
	state_init(&s, argv[1], insert_stmt);

	// Open SQLite database file for querying max date in existing history data
	struct state d;
	state_init2(&d, argv[1]);
	rc = sqlite3_step (d.statement);
	if(rc != SQLITE_ROW) {
		fprintf(stderr, "Error during max date query execution (%s): %s\n", sqlite3_sql(d.statement), sqlite3_errmsg(d.db));
		sqlite3_close(d.db);
		exit(EXIT_FAILURE);
	}	
	rc = sqlite3_column_type(d.statement, 0);
	if (rc == SQLITE_NULL) {
	  
	  
	}
	else
	{
	  
	}  
	printf("%d\n",rc);
	state_finish(&d);

	

/*	
	// Open MySQL Database and read timestamp of the last record written
	if(!mysql_init(&mysql))
	{
		fprintf(stderr, "Cannot initialize MySQL");
		exit(EXIT_FAILURE);
	}
	
	// connect to database
	mysql_connection = mysql_real_connect(&mysql, config.mysql_host, config.mysql_user,
	                                      config.mysql_passwd, config.mysql_database,
	                                      config.mysql_port, NULL, 0);

	if(mysql_connection == NULL)
	{
		fprintf(stderr, "%d: %s \n",
		mysql_errno(&mysql), mysql_error(&mysql));
		exit(EXIT_FAILURE);
	}

*/	
	// By default read all records
	// We set the date to 1 Jan 1990 0:00
	time_lastlog_tm.tm_year = 90;
	time_lastlog_tm.tm_mon = 0;
	time_lastlog_tm.tm_mday = 1;
	time_lastlog_tm.tm_hour = 0;
	time_lastlog_tm.tm_min = 0;
	time_lastlog_tm.tm_sec = 0;
	time_lastlog_tm.tm_isdst = -1;

	time_lastlog = mktime(&time_lastlog_tm);


	// Start reading the history from the WS2300
		
	current_record = read_history_info(ws2300, &interval, &countdown, &time_last,
	                           &no_records);
	                           
	time_lastrecord_tm.tm_year = time_last.year - 1900;
	time_lastrecord_tm.tm_mon  = time_last.month - 1;
	time_lastrecord_tm.tm_mday = time_last.day;
	time_lastrecord_tm.tm_hour = time_last.hour;
	time_lastrecord_tm.tm_min  = time_last.minute;
	time_lastrecord_tm.tm_sec  = 0;
	time_lastrecord_tm.tm_isdst = -1;

	time_lastrecord = mktime(&time_lastrecord_tm);
	
	pressure_term = pressure_correction(ws2300, config.pressure_conv_factor);

	new_records = (int)difftime(time_lastrecord,time_lastlog) / (60 * interval);
	
	if (new_records > 0xAF)
		new_records = 0xAF;
		
	if (new_records > no_records)
		new_records = no_records;

	lastlog_record = current_record - new_records;
	
	if (lastlog_record < 0)
		lastlog_record = 0xAE + lastlog_record + 1;

	time_lastrecord_tm.tm_min -= new_records * interval;

	// Run through the records read
	for (i = 1; i <= new_records; i++)
	{
		next_record = (i + lastlog_record) % 0xAF;
		
		read_history_record(ws2300, next_record, &config,
		                    &temperature_in,
		                    &temperature_out,
		                    &pressure,
		                    &humidity_in,
		                    &humidity_out,
		                    &rain,
		                    &windspeed,
		                    &winddir_degrees,
		                    &dewpoint,
		                    &windchill);

		// Build the second date DB columns -> ws_cal_datetime
		// CALCULATED WEATHERSTATION DATE & TIME
		time_lastrecord_tm.tm_min += interval;
		mktime(&time_lastrecord_tm);                 //normalize time_lastlog_tm
		strftime(datestring, sizeof(datestring), "%Y-%m-%d %H:%M:%S", &time_lastrecord_tm);
		rc = sqlite3_bind_text(s.statement, sqlite3_bind_parameter_index(s.statement, ":ws_cal_datetime"), datestring, -1, SQLITE_STATIC);
		check_rc(&s, rc);

		// Build the first date DB columns -> sys_read_datetime 
		// CURRENT LOCAL DATE & TIME 
		time(&rt);
		strftime(rtstring, sizeof(rtstring), "%Y-%m-%d %H:%M:%S", localtime(&rt));
		rc = sqlite3_bind_text(s.statement, sqlite3_bind_parameter_index(s.statement, ":sys_read_datetime"), rtstring, -1, SQLITE_STATIC);
		check_rc(&s, rc);

		// INDOOR TEMPERATURE
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":temperature_in"), temperature_in);
		check_rc(&s, rc);

		// OUTDOOR TEMPERATURE
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":temperature_out"), temperature_out);
		check_rc(&s, rc);

		// DEWPOINT
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":dewpoint"), dewpoint);
		check_rc(&s, rc);

		// RELATIVE HUMIDITY INDOOR
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":rel_humidity_in"), humidity_in);
		check_rc(&s, rc);

		// RELATIVE HUMIDITY OUTDOOR
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":rel_humidity_out"), humidity_out);
		check_rc(&s, rc);

		// READ WIND SPEED
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":wind_speed"), windspeed);
		check_rc(&s, rc);

		// WIND DEGREE
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":wind_angle"), winddir_degrees);
		check_rc(&s, rc);

		// WIND DIRECTION
		rc = sqlite3_bind_text(s.statement, sqlite3_bind_parameter_index(s.statement, ":wind_direction"), directions[(int)(winddir_degrees/22.5)], -1, SQLITE_STATIC);
		check_rc(&s, rc);
		
		// WINDCHILL
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":wind_chill"), windchill);
		check_rc(&s, rc);

		// RAIN TOTAL
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":rain_total"), rain);
		check_rc(&s, rc);

		// RELATIVE PRESSURE
		rc = sqlite3_bind_double(s.statement, sqlite3_bind_parameter_index(s.statement, ":rel_pressure"), pressure + pressure_term);
		check_rc(&s, rc);

		/* Run the query */
		state_reinit(&s, columns);

/*		
		// If humidity is > 100 the record is skipped
		if(humidity_out < 100)
		{
			strftime(datestring,sizeof(datestring),"\"%Y-%m-%d %H:%M:%S\"",
			         &time_lastrecord_tm);
			// Line up all value in order of appearance in the database
			sprintf(mysql_values_stmt," VALUES(%s", datestring);
			sprintf(mysql_values_stmt,"%s,%.1f",mysql_values_stmt, temperature_in);
			sprintf(mysql_values_stmt,"%s,%.1f",mysql_values_stmt, temperature_out);
			sprintf(mysql_values_stmt,"%s,%.1f",mysql_values_stmt, dewpoint);
			sprintf(mysql_values_stmt,"%s,%d",mysql_values_stmt, humidity_in);
			sprintf(mysql_values_stmt,"%s,%d",mysql_values_stmt, humidity_out);
			sprintf(mysql_values_stmt,"%s,%.1f ",mysql_values_stmt, windspeed);
			sprintf(mysql_values_stmt,"%s,%.1f,\"%s\"",mysql_values_stmt,
			        winddir_degrees, directions[(int)(winddir_degrees/22.5)]);
			sprintf(mysql_values_stmt,"%s,%.1f",mysql_values_stmt, windchill);
			sprintf(mysql_values_stmt,"%s,%.2f",mysql_values_stmt, rain);
			sprintf(mysql_values_stmt,"%s,%.3f)",mysql_values_stmt, pressure + pressure_term);

			// Build SQL string
			sprintf(mysql_stmt,"%s %s",mysql_insert_stmt, mysql_values_stmt);

			// Push to database	
			mysql_state = mysql_query(mysql_connection, mysql_stmt);

			// Error no 1062 means record is already stored
			// We only report other errors
			if((mysql_state != 0) && (mysql_errno(&mysql) != 1062))
			{
				// Something went wrong
				// Just print error message and move ahead
				fprintf(stderr, "Could not insert row. %d: \%s \nStatement was : %s\n",
				        mysql_errno(&mysql), mysql_error(&mysql),mysql_stmt);
			}
		}
		else
		{
			strftime(datestring,sizeof(datestring),"%Y-%m-%d %H:%M:%S",
			         &time_lastrecord_tm);
			fprintf(stderr, "Humidity is %d. Dataset for %s skipped.\n",humidity_out, datestring);
		}
*/
	
	}

	// Goodbye and Goodnight
	close_weatherstation(ws2300);
	state_finish(&s);
	return(EXIT_SUCCESS);
/*
	mysql_close(&mysql);
	return(0);
*/
} 
