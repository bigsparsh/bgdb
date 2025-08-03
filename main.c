#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>

#define COLUMN_USERNAME_SIZE 40
#define COLUMN_EMAIL_SIZE 40
#define TABLE_MAX_PAGES 100

#define size_of_attributes(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef struct {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
} row;

const uint32_t ID_SIZE = size_of_attributes(row, id);
const uint32_t USERNAME_SIZE = size_of_attributes(row, username);
const uint32_t EMAIL_SIZE = size_of_attributes(row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


typedef struct {
	char* buffer;
	size_t buffer_length;
	ssize_t buffer_size;
} input_buffer;


typedef struct {
	uint32_t num_rows;
	void *pages[TABLE_MAX_PAGES];
} table;

typedef enum {
	COMMAND_SUCCESS,
	COMMAND_UNRECOGNIZED
} command_result;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} execute_result;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_NEGATIVE_ID,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR
} prepare_result;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} statement_type;

typedef struct {
	statement_type type;
	row row_to_insert;
} statement;


void kill_input_buffer (input_buffer* input_bfr) {
	free(input_bfr->buffer);
	free(input_bfr);
}

command_result do_command (input_buffer *input_bfr) {
	if (strcmp(input_bfr->buffer, ".exit") == 0) {
		exit(EXIT_SUCCESS);
	} else {
		return (COMMAND_UNRECOGNIZED);
	}

}

void print_prompt () {
	printf("bgdb > ");
}

void read_input (input_buffer* input_bfr) {
	ssize_t bytes_read = getline(&(input_bfr->buffer), &(input_bfr->buffer_length), stdin);

	if (bytes_read <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	input_bfr->buffer_length = bytes_read - 1;
	input_bfr->buffer[bytes_read - 1] = 0;

	// printf("\nBytes Read: %zu", bytes_read);
	// printf("\nInput Buffer: %s", input_bfr->buffer);
	// printf("\nInput length: %zu", input_bfr->buffer_length);
	// printf("\nInput Size: %zu\n\n", input_bfr->buffer_size);
}

void serialize_row(row* src, void* dest) {
	memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
	memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
	memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row (void* src, row* dest) {
	memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
	memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot (table *tbl, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void* page = tbl->pages[page_num];

	if (page == NULL) page = tbl->pages[page_num] = malloc(PAGE_SIZE);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}


input_buffer* create_input_buffer () {
	input_buffer* input_bfr = malloc(sizeof(input_buffer));
	input_bfr->buffer = NULL;
	input_bfr->buffer_length = 0;
	input_bfr->buffer_size = 0;

	return input_bfr;
}

prepare_result prepare_insert (input_buffer *input_bfr, statement *stmt) {
	stmt->type = STATEMENT_INSERT;

	char *keyword = strtok(input_bfr->buffer, " ");
	char *id_string = strtok(NULL, " ");
	char *username = strtok(NULL, " ");
	char *email = strtok(NULL, " ");

	if (id_string == NULL || username == NULL || email == NULL) {
		return PREPARE_SYNTAX_ERROR;
	}

	int id = atoi(id_string);
	if (id < 0) {
		return PREPARE_NEGATIVE_ID;
	}
	if (strlen(username) > COLUMN_USERNAME_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	if (strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}

	stmt->row_to_insert.id = id;
	strcpy(stmt->row_to_insert.username, username);
	strcpy(stmt->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}

prepare_result prepare_statement (input_buffer *input_bfr, statement *stmt) {
	if (strncmp(input_bfr->buffer, "select", 6) == 0) {
		stmt->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	} 
	if (strncmp(input_bfr->buffer, "insert", 6) == 0) {
		return prepare_insert(input_bfr, stmt);
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void print_row (row* r) {
	printf("( ID: %d, Username: %s, Email: %s )\n", r->id, r->username, r->email);
}

execute_result execute_insert (statement *stmt, table *tbl) {
	if (tbl->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}
	row* row_to_insert = &(stmt->row_to_insert);
	serialize_row(row_to_insert, row_slot(tbl, tbl->num_rows));
	tbl->num_rows += 1;
	return EXECUTE_SUCCESS;
}

execute_result execute_select (statement *stmt, table* tbl) {
	row r;
	for (int i = 0; i < tbl->num_rows; i++) {
		deserialize_row(row_slot(tbl, i), &r);
		print_row(&r);
	}
	return EXECUTE_SUCCESS;
}

execute_result execute_statement (statement *stmt, table *tbl) {
	switch (stmt->type) {
		case STATEMENT_INSERT:
		  return execute_insert(stmt, tbl);
		case STATEMENT_SELECT:
		  return execute_select(stmt, tbl);
	}

	return EXECUTE_SUCCESS;
}

table* new_table () {
	table* tbl = ( table* ) malloc(sizeof(table));
	tbl->num_rows = 0;

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		tbl->pages[i] = NULL;
	}

	return tbl;
}

void free_table (table *tbl) {
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		free( tbl->pages[i] );
	}
	free(tbl);
}

int main () {

	table *tbl = new_table();
	input_buffer* input_bfr = create_input_buffer();

	while (true) {
		print_prompt ();
		read_input (input_bfr);

		if (strcmp(input_bfr->buffer, ".exit") == 0) {
			if (input_bfr->buffer[0] == '.') {
				switch (do_command(input_bfr)) {
					case COMMAND_SUCCESS:
						continue;
					case COMMAND_UNRECOGNIZED:
						printf("Unrecognized command '%s'.\n", input_bfr->buffer);
					  continue;
				}
			}
		}

		statement stmt;
		switch (prepare_statement(input_bfr, &stmt)) {
			case PREPARE_SUCCESS:
			  break;
			case PREPARE_SYNTAX_ERROR:
			  printf("Syntax error. Could not parse statement.\n");
			  continue;
			case PREPARE_STRING_TOO_LONG:
			  printf("String is too long.\n");
			  continue;
			case PREPARE_UNRECOGNIZED_STATEMENT:
			  printf("Unrecognized keyword at the start of '%s'.\n", input_bfr->buffer);
			  continue;
			case PREPARE_NEGATIVE_ID:
				printf("ID must be positive.\n");
				continue;
		}


		switch (execute_statement(&stmt, tbl)) {
			case EXECUTE_SUCCESS:
			  printf("Executed.\n");
				break;
			case EXECUTE_TABLE_FULL:
				printf("Error: Table is full.\n");
				break;
		}

	}

	return 0;
}
