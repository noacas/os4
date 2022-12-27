#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <limits.h>
#include <errno.h>
#include <threads.h>

#define PERMISSION_DENIED 2
#define DIR_PATH_IS_FILE 3
#define HANDOFF_TO_NO_ONE -1

typedef struct dir_node {
    char path[PATH_MAX];
    struct dir_node *next;
} dir_node;

typedef struct dir_queue {
    dir_node *first;
    dir_node *last;
} dir_queue;

static int number_of_threads;
static char *search_term;
static dir_queue queue;
static long *threads_queue;
static int thread_queue_first;
static int thread_queue_last;
static int thread_queue_capacity;
_Atomic int number_of_files = 0;
_Atomic int error_in_thread = 0;
mtx_t count_ready_threads_mutex;
cnd_t count_ready_threads_cv;
static int ready_threads = 0;
cnd_t start_all_threads_cv;
mtx_t queue_mutex;
mtx_t all_threads_are_idle_mutex;
cnd_t all_threads_are_idle_cv;
cnd_t *threads_cv;
cnd_t priority_thread_is_done_cv;
static long handoff_to = HANDOFF_TO_NO_ONE;
static int all_threads_need_to_exit;

int thread_main(void *thread_param);
void wait_for_wakeup();
int insert_dir_path_to_queue(char *dir_path);
char* pop_from_queue(long thread_number);
// these should only be accessed with queue_mutex
void register_thread_to_queue(long thread_number);
void wake_up_thread_if_needed();
int get_threads_queue_size();

int get_threads_queue_size() {
    if (thread_queue_last >= thread_queue_first) {
        return thread_queue_last - thread_queue_first;
    }
    return thread_queue_last + thread_queue_capacity - thread_queue_first;
}

void register_thread_to_queue(long thread_number) {
    threads_queue[thread_queue_last] = thread_number;
    thread_queue_last = (thread_queue_last + 1) % thread_queue_capacity;
    if (get_threads_queue_size() == number_of_threads) {
        cnd_signal(&all_threads_are_idle_cv);
        mtx_unlock(&queue_mutex);
        thrd_exit(EXIT_SUCCESS);
    }
}

int insert_dir_path_to_queue(char *dir_path) {
    int fd;
    struct stat entry_stats;
    if (lstat(dir_path, &entry_stats) != 0){
        fprintf(stderr, "Failed to get stats on %s: %s\n", dir_path, strerror(errno));
        return EXIT_FAILURE;
    }
    else if (!S_ISDIR(entry_stats.st_mode)) {
        return DIR_PATH_IS_FILE;
    }
    fd = access(dir_path, F_OK);
    if(fd == -1){
        fprintf(stderr, "Directory %s: Permission denied.\n", dir_path);
        return PERMISSION_DENIED;
    }
    dir_node *new_node = malloc(sizeof(dir_node));
    if (new_node == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        return EXIT_FAILURE;
    }
    strcpy(new_node->path, dir_path);
    mtx_lock(&queue_mutex);
    // let thread with priority pop from query (queue is not empty) before letting other threads to insert to queue
    while (handoff_to != HANDOFF_TO_NO_ONE) {
        cnd_wait(&priority_thread_is_done_cv, &queue_mutex);
    }
    if (queue.last != NULL) {
        new_node->next = NULL;
        queue.last->next = new_node;
        queue.last = new_node;
    } else {
        queue.last = new_node;
        queue.first = new_node;
    }

    wake_up_thread_if_needed();

    mtx_unlock(&queue_mutex);
    return EXIT_SUCCESS;
}

char *pop_from_queue(long thread_number) {
    char *dir_path;
    mtx_lock(&queue_mutex);
    dir_node *node = queue.first;
    while (node == NULL || (handoff_to != HANDOFF_TO_NO_ONE && handoff_to != thread_number) ) {
        // wait until full or until all waiting threads are done
        register_thread_to_queue(thread_number);
        cnd_wait(&threads_cv[thread_number], &queue_mutex);
        if (all_threads_need_to_exit == 1) {
            mtx_unlock(&queue_mutex);
            thrd_exit(EXIT_SUCCESS);
        }
        node = queue.first;
    }
    node = queue.first;
    //printf("before node ptr %p\n", node);
    queue.first = node->next;
    if (queue.last == node) {
        queue.last = NULL;
    }
    //printf("after node ptr\n");
    handoff_to = HANDOFF_TO_NO_ONE; // giving up on priority
    cnd_broadcast(&priority_thread_is_done_cv);
    mtx_unlock(&queue_mutex);
    dir_path = calloc(strlen(node->path) + 1, sizeof(char));
    if (dir_path == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        return NULL;
    }
    strcpy(dir_path, node->path);
    //printf("trying to free node %p, %s\n", node, node->path);
    //free(node);
    return dir_path;
}

void wake_up_thread_if_needed() {
    if (get_threads_queue_size() == 0) {
        return;
    }
    long thread_number_to_wake = threads_queue[thread_queue_first];
    thread_queue_first = (thread_queue_first + 1) % thread_queue_capacity;
    handoff_to = thread_number_to_wake; // giving priority to the thread
    cnd_signal(&threads_cv[thread_number_to_wake]);
}


void wait_for_wakeup() {
    //sleep and broadcast to wake up all threads after they are all ready
    mtx_lock(&count_ready_threads_mutex);
    ready_threads++;
    if (ready_threads == number_of_threads) {
        cnd_signal(&count_ready_threads_cv);
    }
    while (ready_threads != number_of_threads) {
        cnd_wait(&start_all_threads_cv, &count_ready_threads_mutex);
    }
    mtx_unlock(&count_ready_threads_mutex);
}

int thread_main(void *thread_param) {
    long thread_number = (long)thread_param;
    struct dirent *dp;
    char * dir_path;
    DIR * dir;
    char new_path[PATH_MAX];

    wait_for_wakeup();

    while (1) {
        dir_path = pop_from_queue(thread_number);
        if (dir_path == NULL) {
            error_in_thread = 1;
            continue;
        }
        dir = opendir(dir_path);
        if(dir == NULL){
            fprintf(stderr, "Failed to open directory %s: %s\n", dir_path, strerror(errno));
            return EXIT_FAILURE;
        }
        while ((dp = readdir(dir)) != NULL) {
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
                continue;
            }
            // create string of file path
            strcpy(new_path, dir_path);
            strcat(new_path, "/");
            strcat(new_path, dp->d_name);
            printf("the file is %s\n", new_path);
            switch (insert_dir_path_to_queue(new_path)) {
                case EXIT_FAILURE:
                    error_in_thread = 1;
                    continue;
                    break;
                case DIR_PATH_IS_FILE:
                    if (strstr(dp->d_name, search_term) != NULL) {
                        // number of files is atomic
                        number_of_files++;
                        printf("%s\n", new_path);
                    }
                    break;
            }
        }
        closedir(dir);
        //(dir_path);
    }
    thrd_exit(EXIT_SUCCESS);
}


int main(int argc, char *argv[]) {
    int rc;
    thrd_t *thread_ids;

    if (argc != 4) {
        fprintf(stderr, "Usage: <search root directory> <search term> <number of searching threads>\n");
        exit(EXIT_FAILURE);
    }

    if (insert_dir_path_to_queue(argv[1]) != EXIT_SUCCESS) {
        exit(EXIT_FAILURE);
    }

    number_of_threads = atoi(argv[3]);
    search_term = argv[2];
    thread_ids = calloc(number_of_threads, sizeof(thrd_t));
    if (thread_ids == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }
    // threads waiting queue is a circular list
    thread_queue_capacity = number_of_threads + 1;
    threads_queue = calloc(thread_queue_capacity, sizeof (long));

    // init mutex and cv for starting threads
    mtx_init(&count_ready_threads_mutex, mtx_plain);
    cnd_init(&count_ready_threads_cv);
    cnd_init(&start_all_threads_cv);
    //init mutex and cv for waiting queue
    mtx_init(&queue_mutex, mtx_plain);
    cnd_init(&all_threads_are_idle_cv);
    mtx_init(&all_threads_are_idle_mutex, mtx_plain);
    cnd_init(&priority_thread_is_done_cv);
    threads_cv = calloc(number_of_threads, sizeof(cnd_t));
    if (threads_cv == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }
    for (long i = 0; i < number_of_threads; i++) {
        cnd_init(&threads_cv[i]);
    }

    for (long i = 0; i < number_of_threads; i++) {
        rc = thrd_create(&thread_ids[i], thread_main, (void *) i);
        if (rc != thrd_success) {
            fprintf(stderr, "Failed creating thread\n");
            exit(EXIT_FAILURE);
        }
    }

    // wait for all threads to be created
    mtx_lock(&count_ready_threads_mutex);
    while (ready_threads < number_of_threads) {
        cnd_wait(&count_ready_threads_cv, &count_ready_threads_mutex);
    }
    mtx_unlock(&count_ready_threads_mutex);

    // wake all threads up
    cnd_broadcast(&start_all_threads_cv);

    // kill mutex and cv after starting threads
    mtx_destroy(&count_ready_threads_mutex);
    cnd_destroy(&count_ready_threads_cv);
    cnd_destroy(&start_all_threads_cv);

    // wait for all threads to be idle
    mtx_lock(&all_threads_are_idle_mutex);
    cnd_wait(&all_threads_are_idle_cv, &all_threads_are_idle_mutex);
    mtx_unlock(&all_threads_are_idle_mutex);
    mtx_destroy(&all_threads_are_idle_mutex);
    cnd_destroy(&all_threads_are_idle_cv);

    // wake up all threads and get them to die
    all_threads_need_to_exit = 1;
    for (long i = 0; i < number_of_threads; i++) {
        cnd_signal(&threads_cv[i]);
    }

    for (long i = 0; i < number_of_threads; i++) {
        thrd_join(thread_ids[i], NULL);
    }

    printf("Done searching, found %d files\n", number_of_files);

    return error_in_thread;
}