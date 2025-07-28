#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct {
	char* buffer;
	size_t buffer_length;
	ssize_t buffer_size;
} input_buffer;

typedef enum {
	COMMAND_SUCCESS,
	COMMAND_UNRECOGNIZED
} command_result;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
} prepare_result;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} statement_type;

typedef struct {
	statement_type type;
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


input_buffer* create_input_buffer () {
	input_buffer* input_bfr = malloc(sizeof(input_buffer));
	input_bfr->buffer = NULL;
	input_bfr->buffer_length = 0;
	input_bfr->buffer_size = 0;

	return input_bfr;
}

prepare_result prepare_statement (input_buffer *input_bfr, statement *stmt) {
	if (strncmp(input_bfr->buffer, "select", 6) == 0) {
		stmt->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	} 
	if (strncmp(input_bfr->buffer, "insert", 6) == 0) {
		stmt->type = STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement (statement *stmt) {
	switch (stmt->type) {
		case STATEMENT_INSERT:
			printf("This is the insert logic.\n");
			break;
		case STATEMENT_SELECT:
			printf("This is the select logic.\n");
			break;
	}
}

int main () {

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
			case PREPARE_UNRECOGNIZED_STATEMENT:
			  printf("Unrecognized keyword at the start of '%s'.\n", input_bfr->buffer);
				continue;
		}

		execute_statement(&stmt);
		printf("Executed.\n");

	}

	return 0;
}
