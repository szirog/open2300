/*  
 *  open2300 - sqlitehistlog2300.c
 *  
 *  Open2300 1.11  - sqlitehistlog2300 1.0
 *
 *  Get history data from WS2300 weather station
 *  and the new history records store in an SQLite database.
 *  The seconds function of this tool the WS-23XX date and time
 *  synchronization to system local or UTC/GMT time before check
 *  the new history data (optional).
 *
 *  This program is published under the GNU General Public License 
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

/* Forked from the modified sqlitelog2300.c source */
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
	printf("\nsqlitehistlog2300 - Log history data from WS-2300 to SQLite.\n");
	printf("Version %s (C)2013 SziroG\n", VERSION);
	printf("This program is released under the GNU General Public License (GPL)\n\n");
	printf("Usage:\n");
	printf("sqlitehistlog2300 sqlite_db_filename [config_filename] [sync_ws_datetime: lct -> sync to system local time, utc -> sync to UTC time]\n");
	printf("\n");
	exit(0);
}

/* Forked from the modified sqlitelog2300.c source */
/********************************************************************
 * The state struct stores the state of this program. It exists to
 * allow clean shutdown in the event an error ocurrs.
 *
 ********************************************************************/
struct state 
{
	sqlite3 *db;
	sqlite3_stmt *statement;
};

/* QUERY_BUF_SIZE specifies the size in bytes for the SQL INSERT statement
 * that is built */
#define QUERY_BUF_SIZE 4096

/* Forked from the modified sqlitelog2300.c source */
/********************************************************************
 * state_init initializes a state struct
 *
 * Input:	Pointer to state structure to initialize
 *			The path to the SQLite database
 * 		    Null terminated string that contains the actual query.
 *
 * Output:	Initialized state structure
 *
 * Returns: void
 *
 ********************************************************************/
void state_init(struct state* state, const char *db_path, char *query)
{
	int rc;

	/* Connect to the database */
	rc = sqlite3_open(db_path, &state->db);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "\nUnable to open database (%s): %s\n", db_path, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}

	/* Prepare the query */
	int nByte = -1;
	rc = sqlite3_prepare_v2(state->db, query, nByte, &state->statement, NULL);
	if(rc != SQLITE_OK) {
		fprintf(stderr, "\nUnable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
}

/* New procedure */
/********************************************************************
 * state_reinit reinitializes a state struct
 *
 * Input:	Pointer to state structure to initialize
 * 		    Null terminated string that contains the actual query.
 *
 * Output:	Initialized state structure
 *
 * Returns: void
 *
 ********************************************************************/
void state_reinit(struct state* state, char *query)
{
	int rc;
	// Puts binded values to database
	rc = sqlite3_step(state->statement);
	if (rc != SQLITE_DONE) 
	{
		fprintf(stderr, "\nUnable to execute query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
	rc = sqlite3_finalize(state->statement);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "\nUnable to execute query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}

	/* Prepare the query again  */
	int nByte = -1;
	rc = sqlite3_prepare_v2(state->db, query, nByte, &state->statement, NULL);
	if(rc != SQLITE_OK) 
	{
		fprintf(stderr, "\nUnable to prepare query (%s): %s\n", query, sqlite3_errmsg(state->db));
		sqlite3_close(state->db);
		exit(EXIT_FAILURE);
	}
}


/* Forked from the modified sqlitelog2300.c source */
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
}

/* Forked from the modified sqlitelog2300.c source */
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
	if(rc != SQLITE_OK) 
	{
		fprintf(stderr, "\nUnable to bind value: %s\n", sqlite3_errmsg(state->db));
		state_finish(state);
		exit(EXIT_FAILURE);
	}
}

/* New procedure */
/********************************************************************
 * state_reinit reinitializes a state struct
 *
 * Input:	Pointer to state structure to initialize
 * 		    Null terminated string that contains the actual query.
 *
 * Output:	Initialized state structure
 *
 * Returns: void
 *
 ********************************************************************/
void check_maxretries(int no, char *msg)
{
		if (no==MAXRETRIES)
		{
			fprintf(stderr, msg);
			exit(EXIT_FAILURE);
		}
}



/********** MAIN PROGRAM ************************************************
 *
 * This program reads the history records from a WS2300
 * weather station at a given record range
 * and puts the data to a SQLite database file.
 * Just run the program without parameters for usage.
 *
 * It uses the config file for device name.
 * Config file locations - see open2300.conf-dist
 *
 ***********************************************************************/
int main(int argc, char *argv[])
{
	WEATHERSTATION ws2300;
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
	const char * directions[]= {"N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"};

	char * columns[] = 
	{
		"sys_datetime",
		"ws_datetime",
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

	int i, rc;
	time_t rt;
	struct tm * wst;
	char rtstring[50];
	char * select_stmt = "SELECT datetime(MAX(ws_datetime)) FROM weather_history";
	char insert_stmt[QUERY_BUF_SIZE + 1] = ""; /* +1 for trailing NUL */
	struct state s;
	const char * ws_localtime_sync = "lct";
	const char * ws_utctime_sync = "utc";
	char tempchar[] = "0";
	unsigned char data[6], tmpdata[6];
	unsigned char command[25];
	const int ws_time_addr = 0x0200;
	const int ws_date_addr = 0x024D;
	const int ws_datetime_addr = 0x023B;
	char * tmpstr;
	
	// Check the running parameters
	switch (argc) 
	{
		case   4:
			if ((strncmp(argv[3],ws_localtime_sync,strlen(argv[3])) == 0) || (strncmp(argv[3],ws_utctime_sync,strlen(argv[3])) == 0))
			{
				time(&rt);
				if ( strncmp(argv[3],ws_localtime_sync,strlen(argv[3])) == 0 )
				wst = localtime(&rt);
				else wst = gmtime(&rt);
			} 
			else 
			{
				print_usage();
				exit(2);
			}	
		case   3:
			if (get_configuration(&config, argv[2]) == -1)
			{ 
				print_usage();
				exit(2);
			}
			else
			{
			if ((strncmp(argv[2],ws_localtime_sync,strlen(argv[2])) == 0) || (strncmp(argv[2],ws_utctime_sync,strlen(argv[2])) == 0)) 
				{
					time(&rt);
					if ( strncmp(argv[2],ws_localtime_sync,strlen(argv[2])) == 0 ) 
					wst = localtime(&rt);
					else wst = gmtime(&rt);
				} 			
			}
			break;
		case   2:
			if (get_configuration(&config, NULL) == -1)
			{ 
				print_usage();
				exit(2);
			}
			break;
		default	:
			print_usage();
			exit(2);
	}	

	// Setup WS23XX serial port
	ws2300 = open_weatherstation(config.serial_device_name);

	if ( (strncmp(argv[argc-2],ws_localtime_sync,strlen(argv[argc-2])) == 0) || (strncmp(argv[argc-2],ws_utctime_sync,strlen(argv[argc-2])) == 0) ||
	     (strncmp(argv[argc-1],ws_localtime_sync,strlen(argv[argc-1])) == 0) || (strncmp(argv[argc-1],ws_utctime_sync,strlen(argv[argc-1])) == 0)    ) 
	// Sync WS23XX date & time before reading the history
	{
		//strftime(datestring, sizeof(datestring), "%Y-%m-%d %H:%M:%S %Z %z", wst); // this command line is only for testing !!!
		//printf ("%s\n", datestring); // this command line is only for testing !!!
		// Correct and convert date
		wst->tm_year += 1900;
		wst->tm_mon++;
		sprintf( (char*) data, "%02d%02d%02d", (int)(wst->tm_year % 100), wst->tm_mon, wst->tm_mday);
		sprintf( (char*) tmpdata, "%c%c%c%c%c%c", data[ 5 ], data[ 4 ], data[ 3 ], data[ 2 ], data[ 1 ], data[ 0 ]);
		// printf("%s\n",data); // this command line is only for testing !!!
		for (i=0;i<6;i++)
		{
			// Convert each byte in string from ASCII to char
			tempchar[0]=tmpdata[i];
			data[i]=(char)strtol(tempchar,NULL,16);
		}
		// Retrying as ws2300 often fails communication when its cpu is busy
		// We retry until success or MAXRETRIES
		for(i=0;i<MAXRETRIES;i++)
		{
			reset_06(ws2300);
			
			// Write data sends the address and data to WS2300 and
			// returns number of successfully written data nibbles
			// We have success if we have sent all the ones we wanted to send
			
			
			if (write_data(ws2300, ws_date_addr, 6, WRITENIB, data, command) == 6)
			{
				break;
			}
		}
		
		// If we have tried MAXRETRIES times to read we expect not to
		// have valid data
		check_maxretries( i, "\nError syncing date - data writing error\n");

		// Convert time
		sprintf( (char*) data, "%02d%02d%02d", wst->tm_hour, wst->tm_min, wst->tm_sec );
		sprintf( (char*) tmpdata, "%c%c%c%c%c%c", data[ 5 ], data[ 4 ], data[ 3 ], data[ 2 ], data[ 1 ], data[ 0 ]);
		// printf("%s\n",data); // this command line is only for testing !!!
		for (i=0;i<6;i++)
		{
			// Convert each byte in string from ASCII to char
			tempchar[0]=tmpdata[i];
			data[i]=(char)strtol(tempchar,NULL,16);
		}
		// Retrying as ws2300 often fails communication when its cpu is busy
		// We retry until success or MAXRETRIES
		for(i=0;i<MAXRETRIES;i++)
		{
			reset_06(ws2300);
			
			// Write data sends the address and data to WS2300 and
			// returns number of successfully written data nibbles
			// We have success if we have sent all the ones we wanted to send
			
			
			if (write_data(ws2300, ws_time_addr, 6, WRITENIB, data, command) == 6)
			{
				break;
			}
		}
		
		// If we have tried MAXRETRIES times to read we expect not to
		// have valid data
		check_maxretries( i, "\nError syncing time - data writing error\n");
		
		// Check written date and time
		for(i=0;i<MAXRETRIES;i++)
		{
			reset_06(ws2300);
		
			// Read the data. If expected number of bytes read break out of loop.
			if (read_data(ws2300, ws_datetime_addr, 6, data, command) == 6)
			{
				break;
			}
		}
	
		// If we have tried MAXRETRIES times to read we expect not to
		// have valid data
		check_maxretries( i, "\nError syncing date & time - written data reading error\n");
		
		// 023C 2    Current Time: Minutes BCD 10s 			8	023B 6    Current Time: Minutes BCD 1s			9
		// 023E 0    Current Time: Hours BCD 10s			6	023D 3    Current Time: Hours BCD 1s			7
		// 0240 4    Current day of month: BCD 1s			5	023F 4    Current Weekday: Mon-Sun = 1-7		10
		// 0242 4    Current month: BCD 1s				3	0241 2    Current day of month: BCD 10s			4
		// 0244 3    Current year: BCD 1s last two digits		1	0243 0    Current month: BCD 10s			2
		// 0246 n/a (fix zero)  	   			       "0"	0245 0    Current year: BCD 10s last two digit		0
		if (wst->tm_wday == 0) wst->tm_wday = 7;
 		sprintf( (char*) command, "%02d%02d%02d%02d%02d%d", (int)(wst->tm_year % 100), wst->tm_mon, wst->tm_mday, wst->tm_hour, wst->tm_min, wst->tm_wday);
		sprintf( rtstring, "%c%c%c%c%c%c%c%c%c%c%s%c", command[ 8 ], command[ 9 ], command[ 6 ], command[ 7 ], command[ 5 ], command[ 10 ], 
							       command[ 3 ], command[ 4 ], command[ 1 ], command[ 2 ], "0", command[ 0 ]);
		command[0] = 0;
		for (i=0; i<6;i++)
		{
			sprintf ( (char*) tmpdata, "%02X", data[i] );
			strcat ( (char*) command, (const char*) tmpdata );
		}
		//printf("%s %s \n", rtstring, command); // this command line is only for testing !!!
		if (strncmp( rtstring, (char*) command, 12) != 0)
		{
			fprintf(stderr, "\nError syncing date & time - difference(s) between written and read data\n");
			exit(EXIT_FAILURE);		
		}
	}
	
	// Open SQLite database file for querying maximal date in existing history data
	state_init(&s, argv[1], select_stmt);
	rc = sqlite3_step (s.statement);
	if(rc != SQLITE_ROW) {
		fprintf(stderr, "\Error during max date query execution (%s): %s\n", sqlite3_sql(s.statement), sqlite3_errmsg(s.db));
		sqlite3_close(s.db);
		exit(EXIT_FAILURE);
	}	
	rc = sqlite3_column_type(s.statement, 0);
	if (rc == SQLITE_TEXT) {
		// We have got date, convert it
		tmpstr = (char*) sqlite3_column_text(s.statement, 0);
		sprintf( (char*) tmpdata, "%c%c%c%c", tmpstr[ 0 ], tmpstr[ 1 ], tmpstr[ 2 ], tmpstr[ 3 ]);
		time_lastlog_tm.tm_year = atoi ( (char*) tmpdata ) - 1900;
		sprintf( (char*) tmpdata, "%c%c", tmpstr[ 5 ], tmpstr[ 6 ]);
		time_lastlog_tm.tm_mon = atoi ( (char*) tmpdata ) - 1;
		sprintf( (char*) tmpdata, "%c%c", tmpstr[ 8 ], tmpstr[ 9 ]);
		time_lastlog_tm.tm_mday = atoi ( (char*) tmpdata );
		sprintf( (char*) tmpdata, "%c%c", tmpstr[ 11 ], tmpstr[ 12 ]);
		time_lastlog_tm.tm_hour = atoi ( (char*) tmpdata );
		sprintf( (char*) tmpdata, "%c%c", tmpstr[ 14 ], tmpstr[ 15 ]);
		time_lastlog_tm.tm_min = atoi ( (char*) tmpdata );
		sprintf( (char*) tmpdata, "%c%c", tmpstr[ 17 ], tmpstr[ 18 ]);
		time_lastlog_tm.tm_sec = atoi ( (char*) tmpdata );
		time_lastlog_tm.tm_isdst = -1;
		// printf("%s\n",tmpstr); // this command line is only for testing !!!
	}
	else
	{
		// We haven't got date in the database
		// By default read all records
		// We set the date to 1 Jan 1990 0:00	
		time_lastlog_tm.tm_year = 90;
		time_lastlog_tm.tm_mon = 0;
		time_lastlog_tm.tm_mday = 1;
		time_lastlog_tm.tm_hour = 0;
		time_lastlog_tm.tm_min = 0;
		time_lastlog_tm.tm_sec = 0;
		time_lastlog_tm.tm_isdst = -1;
	}  
	time_lastlog = mktime(&time_lastlog_tm);
	state_finish(&s);

	
	// Build SQLite database query string for new record(s) insertion
	// Ensure there's always a NUL char at the end of the buffer.
	// strncat won't override this char as its beyond QUERY_BUF_SIZE
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

	// Open SQLite database file for new record(s) insertion
	state_init(&s, argv[1], insert_stmt);

	// Start reading the history from the WS23XX
	current_record = read_history_info(ws2300, &interval, &countdown, &time_last, &no_records);
	                           
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
	
	if (new_records > 0xAF) new_records = 0xAF;
		
	if (new_records > no_records) new_records = no_records;

	lastlog_record = current_record - new_records;
	
	if (lastlog_record < 0) lastlog_record = 0xAE + lastlog_record + 1;

	//printf("%d %d %d\n", new_records, lastlog_record, current_record ); // this command line is only for testing !!!
	//printf("%4d-%02d-%02d %02d:%02d:%02d  %4d-%02d-%02d %02d:%02d:%02d\n", time_lastrecord_tm.tm_year + 1900, time_lastrecord_tm.tm_mon + 1, time_lastrecord_tm.tm_mday, // this command line is only for testing !!!
	//								       time_lastrecord_tm.tm_hour, time_lastrecord_tm.tm_min, time_lastrecord_tm.tm_sec, // this command line is only for testing !!!
	//								       time_lastlog_tm.tm_year + 1900, time_lastlog_tm.tm_mon + 1, time_lastlog_tm.tm_mday, // this command line is only for testing !!!
	//								       time_lastlog_tm.tm_hour, time_lastlog_tm.tm_min, time_lastlog_tm.tm_sec); // this command line is only for testing !!!

	time_lastrecord_tm.tm_min -= new_records * interval;

	// Prepare the processing system local date & time value to storing into DB as "sys_datetime"
	time(&rt);
	strftime(rtstring, sizeof(rtstring), "%Y-%m-%d %H:%M:%S", localtime(&rt));
	
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

		// First DB (date) column -> "sys_datetime" 
		// PROCESSING LOCAL SYSTEM DATE & TIME
		rc = sqlite3_bind_text(s.statement, sqlite3_bind_parameter_index(s.statement, ":sys_datetime"), rtstring, -1, SQLITE_STATIC);
		check_rc(&s, rc);

		// Build the second DB (date) column -> "ws_datetime"
		// HISTORY RECORD DATE & TIME STORED BY WEATHERSTATION 
		time_lastrecord_tm.tm_min += interval;
		mktime(&time_lastrecord_tm);                 //normalize time_lastlog_tm
		strftime(datestring, sizeof(datestring), "%Y-%m-%d %H:%M:%S", &time_lastrecord_tm);
		rc = sqlite3_bind_text(s.statement, sqlite3_bind_parameter_index(s.statement, ":ws_datetime"), datestring, -1, SQLITE_STATIC);
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

		/* Post values and reinit the query */
		if ( i != new_records ) 
		state_reinit(&s, insert_stmt);
		else 
		{
			rc = sqlite3_step(s.statement);
			if (rc != SQLITE_DONE) 
			{
				fprintf(stderr, "\nUnable to execute query (%s): %s\n", sqlite3_sql(s.statement), sqlite3_errmsg(s.db));
				sqlite3_close(s.db);
				exit(EXIT_FAILURE);
			}
		}		
	}
	// Goodbye and Goodnight
	state_finish(&s);
	close_weatherstation(ws2300);

    // Output a summary row
	if (wst != NULL)
	{
		strftime(datestring, sizeof(datestring), "%Y-%m-%d %H:%M:%S", wst);
		// We print the following summary row to the standard error output because if the program is started by the CRON 
		// this message will appear in the CRON log file, if CRON started with the "-L"  option
		fprintf(stderr, "\nSQLitehistlog2300 - %s, %d record(s) has written into \"%s\" SQLite database file with time (%s) synchronization in %.1f second(s).\n", datestring, new_records, argv[1], argv[argc-1], difftime(time(NULL),mktime(wst)));
	}
	else
	{
		// We print the following summary row to the standard error output because if the program is started by the CRON 
		// this message will appear in the CRON log file, if CRON started with the "-L"  option
		fprintf(stderr, "\nSQLitehistlog2300 - %s, %d record(s) has written into \"%s\" SQLite database file in %.1f second(s).\n", rtstring, new_records, argv[1], difftime(time(NULL),rt));
	}
	return(EXIT_SUCCESS);
} 
