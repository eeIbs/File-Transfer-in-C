#include <stdio.h>
#include "error_print.h"

void error_printer(char *failure_path, char *specific_error) {

    fprintf(stderr, "%s module failed!\n", failure_path);
    fprintf(stderr, "%s\n", specific_error);  

}