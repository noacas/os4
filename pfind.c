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

typedef struct dir_data {
    char path[PATH_MAX];
    DIR *dir;
} dir_data;

typedef struct dir_node {
    dir_data *dir;
    struct dir_node *next;
} dir_node;

typedef struct dir_queue {
    dir_node *first;
    dir_node *last;
} dir_queue;

typedef struct thread_node {
    long thread_number;
    struct thread_node *next;
} thread_node;

typedef struct threads_queue {
    int size;
    thread_node *first;
    thread_node *last;
} threads_queue;

static int number_of_threads;
static char *search_term;
static dir_queue queue;
static threads_queue threads_pop_queue;
_Atomic int number_of_files;
_Atomic int error_in_thread = 0;
mtx_t count_ready_threads_mutex;
cnd_t count_ready_threads_cv;
static int ready_threads = 0;
cnd_t start_all_threads_cv;
mtx_t queue_mutex;
cnd_t all_threads_are_idle_cv;
cnd_t *threads_cv;
static long handoff_to = -1;

int register_thread_to_queue(long thread_number);
int insert_to_queue(dir_data *data);
dir_data* pop_from_queue(long thread_number);
void wake_up_thread_if_needed();
int insert_dir_path_to_queue(char *dir_path);
int thread_main(void *thread_param);
void wait_for_wakeup();

int register_thread_to_queue(long thread_number) {
    fprintf(stderr, "trying to allocate memory\n");
    thread_node * new_node = malloc(sizeof(struct thread_node));
    if (new_node == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        return EXIT_FAILURE;
    }
    printf("setting thread number\n");
    new_node->thread_number=thread_number;
    printf("finding last in threads queue\n");
    if (threads_pop_queue.last != NULL) {
            new_node->next = NULL;
            threads_pop_queue.last->next = new_node;
            threads_pop_queue.last = new_node;
    }
    else {
        threads_pop_queue.last = new_node;
        threads_pop_queue.first = new_node;
    }
    printf("found last in threads queue\n");
    threads_pop_queue.size++;
    if (threads_pop_queue.size == number_of_threads) {
        cnd_signal(&all_threads_are_idle_cv);
    }
    return EXIT_SUCCESS;
}

int insert_to_queue(dir_data *data) {
    printf("thread trying to insert to queue\n");
    dir_node *new_node = malloc(sizeof(struct dir_node));
    if (new_node == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        return EXIT_FAILURE;
    }
    printf("done malloc\n");
    new_node->dir = data;

    mtx_lock(&queue_mutex);
    if (queue.last != NULL) {
        new_node->next = NULL;
        queue.last->next = new_node;
        queue.last = new_node;
    } else {
        queue.last = new_node;
        queue.first = new_node;
    }

    printf("trying to wakeup thread, waiting are %d\n", threads_pop_queue.size);
    wake_up_thread_if_needed();

    mtx_unlock(&queue_mutex);
    return EXIT_SUCCESS;
}

dir_data* pop_from_queue(long thread_number) {
    dir_data *d;
    mtx_lock(&queue_mutex);
    dir_node *node = queue.first;
    while (node == NULL || (handoff_to != -1 && handoff_to != thread_number) ) {
        // wait until full or until all waiting threads are done
        if (register_thread_to_queue(thread_number) == EXIT_FAILURE) {
            return NULL;
        }
        cnd_wait(&threads_cv[thread_number], &queue_mutex);
        node = queue.first;
    }
    queue.first = node->next;
    if (queue.last == node) {
        queue.last = NULL;
    }
    handoff_to = -1; // giving up on priority
    mtx_unlock(&queue_mutex);
    d = node->dir;
    free(node);
    return d;
}

void wake_up_thread_if_needed() {
    if (threads_pop_queue.size == 0) {
        printf("no waiting threads\n");
        return;
    }
    thread_node *node = threads_pop_queue.first;
    threads_pop_queue.first = node->next;
    if (threads_pop_queue.last == node) {
        threads_pop_queue.last = NULL;
    }
    handoff_to = node->thread_number; // giving priority to the thread
    threads_pop_queue.size--;
    printf("waking up thread number %ld\n", handoff_to);
    free(node);
}

int insert_dir_path_to_queue(char *dir_path) {
    DIR *dir;
    int fd;
    dir_data *dir_data;

    fd = access(dir_path, F_OK);
    if(fd == -1){
        fprintf(stderr, "Directory %s: Permission denied.\n", dir_path);
        return PERMISSION_DENIED;
    }

    dir = opendir(dir_path);
    if(dir == NULL){
        fprintf(stderr, "Failed to open directory %s: %s\n", dir_path, strerror(errno));
        return EXIT_FAILURE;
    }

    dir_data = malloc(sizeof(dir_data));
    if (dir_data == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        return EXIT_FAILURE;
    }

    dir_data->dir=dir;
    strcpy(dir_data->path, dir_path);

    return insert_to_queue(dir_data);
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
    struct stat entry_stats;
    struct dirent *dp;
    dir_data *dir_data;
    char new_path[PATH_MAX];

    wait_for_wakeup();
    printf("thread number %ld awaken\n", thread_number);

    while (1) {
        dir_data = pop_from_queue(thread_number);
        if (dir_data == NULL) {
            error_in_thread = 1;
            continue;
        }
        while ((dp = readdir(dir_data->dir)) != NULL) {
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
                continue;
            }
            // create string of file path
            printf("coping path %s/%s\n", dir_data->path, dp->d_name);
            strcpy(new_path, dir_data->path);
            strcat(new_path, "/");
            strcat(new_path, dp->d_name);
            printf("found path %s\n", new_path);
            if (lstat(new_path, &entry_stats) != 0){
                fprintf(stderr, "Failed to get stats on %s: %s\n", new_path, strerror(errno));
                error_in_thread = 1;
            }
            else if (S_ISDIR(entry_stats.st_mode)) {
                if (insert_dir_path_to_queue(new_path) == EXIT_FAILURE) {
                    error_in_thread = 1;
                }
            }
            else if (strstr(dp->d_name, search_term) != NULL) {
                // number of files is atomic
                number_of_files++;
                printf("%s\n", new_path);
            }
        }
        closedir(dir_data->dir);
        free(dir_data);
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

    // init mutex and cv for starting threads
    mtx_init(&count_ready_threads_mutex, mtx_plain);
    cnd_init(&count_ready_threads_cv);
    cnd_init(&start_all_threads_cv);
    //init mutex and cv for waiting queue
    mtx_init(&queue_mutex, mtx_plain);
    cnd_init(&all_threads_are_idle_cv);
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

    printf("Done searching, found %d files\n", number_of_files);

    return error_in_thread;
}