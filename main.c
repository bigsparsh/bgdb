#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
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


// Used for taking input and maintaing the input size and length.
typedef struct {
	char* buffer;
	size_t buffer_length;
	ssize_t buffer_size;
} input_buffer;

// Used as a pager instance for the DB file, maintains the file length and all the pages within that are to be loaded in memory or freed as required.
typedef struct {
	int file_descriptor;
	uint32_t file_length;
	void *pages[TABLE_MAX_PAGES];
} pager;

// A table instance, conatining number of rows and a pager instance.
typedef struct {
	uint32_t num_rows;
	pager* pgr;
} table;

typedef struct {
	table *tbl;
	uint32_t row_num;
	bool end_of_tbl;
} cursor;

// Determining the normal command success or unrecognized.
typedef enum {
	COMMAND_SUCCESS,
	COMMAND_UNRECOGNIZED
} command_result;

// Determinig the execution status of SQL Commands.
typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} execute_result;

// Determining the preparation status for the commands.
typedef enum {
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_NEGATIVE_ID,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR
} prepare_result;

// Defining the type of SQL Statement.
typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} statement_type;

// A statement instance, containing the row instance which is to be inserted as well as the type of the statement.
typedef struct {
	statement_type type;
	row row_to_insert;
} statement;


// Kills the input buffer, i.e, frees the Input Buffer's buffer string and also the Input Buffer instance itself.
void kill_input_buffer (input_buffer* input_bfr) {
	free(input_bfr->buffer);
	free(input_bfr);
}

void pager_flush (pager* pgr, uint32_t page_num, uint32_t size) {
	if (pgr->pages[page_num] == NULL) {
		printf("Tried to flush null page.\n");
		exit(EXIT_FAILURE);
	}

	off_t offset = lseek(pgr->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

	if (offset == -1) {
		printf("Error seeking: %d\n", errno);
		exit(EXIT_FAILURE);
	}

	ssize_t bytes_written = write(pgr->file_descriptor, pgr->pages[page_num], size);

	if (bytes_written == -1) {
		printf("Error writing: %d\n", errno);
		exit(EXIT_FAILURE);
	}
}

// Closes the Database by freeing the pages from the memory.
void db_close (table* tbl) {
	pager* pgr = tbl->pgr;
	uint32_t num_full_pages = tbl->num_rows / ROWS_PER_PAGE;

	for (uint32_t i = 0; i < num_full_pages; i++) {
		if (pgr->pages[i] == NULL) {
			continue;
		}
		pager_flush(pgr, i, PAGE_SIZE);
		free(pgr->pages[i]);
		pgr->pages[i] = NULL;
	}

	uint32_t num_additional_rows = tbl->num_rows % ROWS_PER_PAGE;
	if (num_additional_rows > 0) {
		uint32_t page_num = num_full_pages;
		if (pgr->pages[page_num] != NULL) {
			pager_flush(pgr, page_num, num_additional_rows * ROW_SIZE);
			free(pgr->pages[page_num]);
			pgr->pages[page_num] = NULL;
		}
	}

	int result = close(pgr->file_descriptor);
	if (result == -1) {
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);
	}
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		void *page = pgr->pages[i];
		if (page) {
			free(page);
			pgr->pages[i] = NULL;
		}
	}
	free(pgr);
	free(tbl);
}

cursor* table_start(table* tbl) {
	cursor* crsr = malloc(sizeof(cursor));
	crsr->tbl = tbl;
	crsr->row_num = 0;
	crsr->end_of_tbl = (tbl->num_rows == 0);

	return crsr;
}

cursor* table_end(table* tbl) {
	cursor* crsr = malloc(sizeof(cursor));
	crsr->tbl = tbl;
	crsr->row_num = tbl->num_rows;
	crsr->end_of_tbl = true;

	return crsr;
}

command_result do_command (input_buffer *input_bfr, table* tbl) {
	if (strcmp(input_bfr->buffer, ".exit") == 0) {
		db_close(tbl);
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

void* get_page (pager *pgr, uint32_t page_num) {
	if (page_num > TABLE_MAX_PAGES) {
		printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}
	if (pgr->pages[page_num] == NULL) {
		void *page = malloc(PAGE_SIZE);
		uint32_t num_pages = pgr->file_length / PAGE_SIZE;

		if (pgr->file_length % PAGE_SIZE) num_pages += 1;

		if (page_num <= num_pages) {
			lseek(pgr->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(pgr->file_descriptor, page, PAGE_SIZE);
			if (bytes_read == -1) {
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		}

		pgr->pages[page_num] = page;
	}

	return pgr->pages[page_num];
}

void *cursor_value (cursor *crsr) {
	uint32_t row_num = crsr->row_num;
	uint32_t page_num = row_num / ROWS_PER_PAGE;

	void *page = get_page(crsr->tbl->pgr, page_num);

	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

void cursor_advance (cursor* crsr) {
	crsr-> row_num += 1;
	if (crsr->row_num >= crsr->tbl->num_rows) {
		crsr->end_of_tbl  = true;
	}
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

pager* pager_open (const char* filename) {
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

	if (fd == -1) {
		printf("Unable to open file.\n");
		exit(EXIT_FAILURE);
	}

	off_t file_length = lseek(fd, 0, SEEK_END);

	pager* pgr = malloc(sizeof(pager));
	pgr->file_descriptor = fd;
	pgr->file_length = file_length;

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		pgr->pages[i] = NULL;
	}

	return pgr;
}

table* db_open (const char* filename) {
	pager* pgr = pager_open(filename);
	uint32_t num_rows = pgr->file_length / ROW_SIZE;
	table* tbl = ( table* ) malloc(sizeof(table));

	tbl->pgr = pgr;
	tbl->num_rows = num_rows;

	return tbl;
}



int main (int argc, char* argv[]) {

	if (argc < 2) {
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}

	char *filename = argv[1];
	table *tbl = db_open(filename);
	input_buffer* input_bfr = create_input_buffer();

	while (true) {
		print_prompt ();
		read_input (input_bfr);

		if (strcmp(input_bfr->buffer, ".exit") == 0) {
			if (input_bfr->buffer[0] == '.') {
				switch (do_command(input_bfr, tbl)) {
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
