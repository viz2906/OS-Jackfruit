/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 8192
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 32
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_HARD_LIMIT_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int stop_requested;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    void *child_stack;
    int producer_thread_started;
    pthread_t producer_thread;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_task_t;

static volatile sig_atomic_t g_supervisor_stop = 0;
static volatile sig_atomic_t g_supervisor_child_event = 0;
static volatile sig_atomic_t g_run_client_signal = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *cursor = buf;

    while (len > 0) {
        ssize_t n = write(fd, cursor, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        cursor += n;
        len -= (size_t)n;
    }

    return 0;
}

static void copy_cstr(char *dst, size_t dst_len, const char *src)
{
    if (dst_len == 0)
        return;

    if (src == NULL) {
        dst[0] = '\0';
        return;
    }

    (void)snprintf(dst, dst_len, "%s", src);
}

static int read_all(int fd, void *buf, size_t len)
{
    char *cursor = buf;

    while (len > 0) {
        ssize_t n = read(fd, cursor, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        cursor += n;
        len -= (size_t)n;
    }

    return 0;
}

static void append_message(char *dst, size_t dst_len, size_t *used, const char *fmt, ...)
{
    va_list ap;
    int written;

    if (*used >= dst_len)
        return;

    va_start(ap, fmt);
    written = vsnprintf(dst + *used, dst_len - *used, fmt, ap);
    va_end(ap);

    if (written < 0)
        return;

    if ((size_t)written >= dst_len - *used)
        *used = dst_len - 1;
    else
        *used += (size_t)written;
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int container_is_active(const container_record_t *record)
{
    return record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *iter;

    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (strcmp(iter->id, id) == 0)
            return iter;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *iter;

    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (iter->host_pid == pid)
            return iter;
    }

    return NULL;
}

static int rootfs_is_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *iter;

    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (container_is_active(iter) && strcmp(iter->rootfs, rootfs) == 0)
            return 1;
    }

    return 0;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

static int resolve_log_path(supervisor_ctx_t *ctx,
                            const char *container_id,
                            char *out,
                            size_t out_len)
{
    container_record_t *record;
    int found = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, container_id);
    if (record != NULL) {
        copy_cstr(out, out_len, record->log_path);
        found = 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return found ? 0 : -1;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc == 0)
            break;
        if (rc < 0)
            continue;

        char path[PATH_MAX] = {0};
        int fd;

        if (resolve_log_path(ctx, item.container_id, path, sizeof(path)) != 0)
            continue;

        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_all(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *log_producer_thread(void *arg)
{
    producer_args_t *producer = arg;
    char buf[LOG_CHUNK_SIZE];

    while (1) {
        ssize_t n = read(producer->read_fd, buf, sizeof(buf));
        if (n == 0)
            break;
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        log_item_t item;
        memset(&item, 0, sizeof(item));
        copy_cstr(item.container_id, sizeof(item.container_id), producer->container_id);
        item.length = (size_t)n;
        memcpy(item.data, buf, item.length);

        if (bounded_buffer_push(&producer->ctx->log_buffer, &item) != 0)
            break;
    }

    close(producer->read_fd);
    free(producer);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    int dev_null;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 127;
    }
    close(cfg->log_write_fd);

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount private /");

    if (cfg->nice_value != 0 && setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 127;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 127;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 127;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 127;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    dev_null = open("/dev/null", O_RDONLY);
    if (dev_null >= 0) {
        (void)dup2(dev_null, STDIN_FILENO);
        close(dev_null);
    }

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("exec /bin/sh -c");
    return 127;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD) {
        g_supervisor_child_event = 1;
        return;
    }

    g_supervisor_stop = 1;
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;

    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;

    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static void run_client_signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
        g_run_client_signal = signo;
}

static int ensure_log_dir(void)
{
    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST)
        return -1;
    return 0;
}

static int ensure_container_log_file(const char *path)
{
    int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0)
        return -1;

    close(fd);
    return 0;
}

static int create_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    unlink(CONTROL_PATH);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 32) < 0) {
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return fd;
}

static int launch_container(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            container_record_t **out_record,
                            char *err,
                            size_t err_len)
{
    char resolved_rootfs[PATH_MAX];
    struct stat st;
    container_record_t *record = NULL;
    child_config_t *child_cfg = NULL;
    producer_args_t *producer = NULL;
    int pipefd[2] = {-1, -1};
    void *stack = NULL;
    pid_t child_pid = -1;
    int rc;

    if (realpath(req->rootfs, resolved_rootfs) == NULL) {
        snprintf(err, err_len, "rootfs path is invalid: %.190s", req->rootfs);
        return -1;
    }

    if (stat(resolved_rootfs, &st) < 0 || !S_ISDIR(st.st_mode)) {
        snprintf(err, err_len, "rootfs is not a directory: %.180s", resolved_rootfs);
        return -1;
    }

    if (ensure_log_dir() != 0) {
        snprintf(err, err_len, "failed to create log dir: %s", strerror(errno));
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id_locked(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(err, err_len, "container id already exists: %s", req->container_id);
        return -1;
    }

    if (rootfs_is_in_use_locked(ctx, resolved_rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(err,
                 err_len,
                 "rootfs already used by a running container: %.150s",
                 resolved_rootfs);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    record = calloc(1, sizeof(*record));
    if (record == NULL) {
        snprintf(err, err_len, "calloc failed: %s", strerror(errno));
        return -1;
    }

    copy_cstr(record->id, sizeof(record->id), req->container_id);
    copy_cstr(record->rootfs, sizeof(record->rootfs), resolved_rootfs);
    record->started_at = time(NULL);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    if (ensure_container_log_file(record->log_path) != 0) {
        snprintf(err,
                 err_len,
                 "failed to create log file %.160s: %s",
                 record->log_path,
                 strerror(errno));
        goto fail;
    }

    if (pipe(pipefd) < 0) {
        snprintf(err, err_len, "pipe failed: %s", strerror(errno));
        goto fail;
    }

    child_cfg = calloc(1, sizeof(*child_cfg));
    if (child_cfg == NULL) {
        snprintf(err, err_len, "calloc failed: %s", strerror(errno));
        goto fail;
    }

    copy_cstr(child_cfg->id, sizeof(child_cfg->id), req->container_id);
    copy_cstr(child_cfg->rootfs, sizeof(child_cfg->rootfs), resolved_rootfs);
    copy_cstr(child_cfg->command, sizeof(child_cfg->command), req->command);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    stack = malloc(STACK_SIZE);
    if (stack == NULL) {
        snprintf(err, err_len, "malloc stack failed: %s", strerror(errno));
        goto fail;
    }

    child_pid = clone(child_fn,
                      (char *)stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      child_cfg);
    if (child_pid < 0) {
        snprintf(err, err_len, "clone failed (need root?): %s", strerror(errno));
        goto fail;
    }

    close(pipefd[1]);
    pipefd[1] = -1;

    record->host_pid = child_pid;
    record->state = CONTAINER_RUNNING;
    record->child_stack = stack;
    stack = NULL;

    producer = calloc(1, sizeof(*producer));
    if (producer == NULL) {
        snprintf(err, err_len, "calloc failed: %s", strerror(errno));
        goto fail;
    }

    producer->ctx = ctx;
    producer->read_fd = pipefd[0];
    copy_cstr(producer->container_id, sizeof(producer->container_id), req->container_id);

    rc = pthread_create(&record->producer_thread, NULL, log_producer_thread, producer);
    if (rc != 0) {
        errno = rc;
        snprintf(err, err_len, "pthread_create failed: %s", strerror(errno));
        goto fail;
    }
    record->producer_thread_started = 1;
    producer = NULL;
    pipefd[0] = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  record->id,
                                  record->host_pid,
                                  record->soft_limit_bytes,
                                  record->hard_limit_bytes) != 0) {
            fprintf(stderr,
                    "Warning: monitor register failed for %s (pid %d): %s\n",
                    record->id,
                    record->host_pid,
                    strerror(errno));
        }
    }

    if (out_record != NULL)
        *out_record = record;

    free(child_cfg);
    return 0;

fail:
    if (child_pid > 0) {
        (void)kill(child_pid, SIGKILL);
        (void)waitpid(child_pid, NULL, 0);
    }

    if (producer != NULL) {
        close(producer->read_fd);
        free(producer);
    }

    if (pipefd[0] >= 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);

    if (stack != NULL)
        free(stack);

    if (record != NULL) {
        if (record->child_stack != NULL)
            free(record->child_stack);
        free(record);
    }

    free(child_cfg);
    return -1;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    while (1) {
        int status;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        container_record_t *record;
        int need_join = 0;
        pthread_t join_thread;

        if (pid == 0)
            return;

        if (pid < 0) {
            if (errno == EINTR)
                continue;
            return;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_pid_locked(ctx, pid);
        if (record != NULL) {
            if (WIFEXITED(status)) {
                record->state = CONTAINER_EXITED;
                record->exit_code = WEXITSTATUS(status);
                record->exit_signal = 0;
            } else if (WIFSIGNALED(status)) {
                int sig = WTERMSIG(status);

                if (record->stop_requested) {
                    record->state = CONTAINER_STOPPED;
                } else if (sig == SIGKILL) {
                    record->state = CONTAINER_HARD_LIMIT_KILLED;
                } else {
                    record->state = CONTAINER_KILLED;
                }

                record->exit_code = -1;
                record->exit_signal = sig;
            }

            if (ctx->monitor_fd >= 0) {
                (void)unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid);
            }

            if (record->child_stack != NULL) {
                free(record->child_stack);
                record->child_stack = NULL;
            }

            if (record->producer_thread_started) {
                join_thread = record->producer_thread;
                record->producer_thread_started = 0;
                need_join = 1;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (need_join)
            (void)pthread_join(join_thread, NULL);
    }
}

static void format_start_time(time_t ts, char *out, size_t out_len)
{
    struct tm tm_buf;

    if (localtime_r(&ts, &tm_buf) == NULL) {
        snprintf(out, out_len, "-");
        return;
    }

    if (strftime(out, out_len, "%Y-%m-%d %H:%M:%S", &tm_buf) == 0)
        snprintf(out, out_len, "-");
}

static void fill_ps_response(supervisor_ctx_t *ctx, control_response_t *resp)
{
    size_t used = 0;
    int count = 0;
    container_record_t *iter;

    resp->status = 0;
    memset(resp->message, 0, sizeof(resp->message));
    append_message(resp->message,
                   sizeof(resp->message),
                   &used,
                   "ID             PID    STATE      SOFT_MiB HARD_MiB EXIT SIG STARTED_AT ROOTFS\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        char started_at[32];

        format_start_time(iter->started_at, started_at, sizeof(started_at));
        append_message(resp->message,
                       sizeof(resp->message),
                       &used,
                       "%-14s %-6d %-10s %-8lu %-8lu %-4d %-3d %s %s\n",
                       iter->id,
                       (int)iter->host_pid,
                       state_to_string(iter->state),
                       iter->soft_limit_bytes >> 20,
                       iter->hard_limit_bytes >> 20,
                       iter->exit_code,
                       iter->exit_signal,
                       started_at,
                       iter->rootfs);
        count++;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (count == 0)
        append_message(resp->message, sizeof(resp->message), &used, "No containers tracked.\n");
}

static void fill_logs_response(supervisor_ctx_t *ctx,
                               const char *container_id,
                               control_response_t *resp)
{
    char path[PATH_MAX];
    int fd;
    size_t used = 0;

    memset(path, 0, sizeof(path));
    if (resolve_log_path(ctx, container_id, path, sizeof(path)) != 0)
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, container_id);

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to read logs for %s (%s): %s\n",
                 container_id,
                 path,
                 strerror(errno));
        return;
    }

    resp->status = 0;
    memset(resp->message, 0, sizeof(resp->message));
    append_message(resp->message,
                   sizeof(resp->message),
                   &used,
                   "Logs for %s (%s):\n",
                   container_id,
                   path);

    while (used < sizeof(resp->message) - 1) {
        ssize_t n = read(fd, resp->message + used, sizeof(resp->message) - 1 - used);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            resp->status = 1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Failed to read logs for %s: %s\n",
                     container_id,
                     strerror(errno));
            close(fd);
            return;
        }
        if (n == 0)
            break;
        used += (size_t)n;
    }

    resp->message[sizeof(resp->message) - 1] = '\0';
    close(fd);
}

static void handle_start_or_run(supervisor_ctx_t *ctx,
                                const control_request_t *req,
                                control_response_t *resp,
                                int wait_for_exit)
{
    container_record_t *record = NULL;
    char err[256];

    memset(err, 0, sizeof(err));
    if (launch_container(ctx, req, &record, err, sizeof(err)) != 0) {
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "start failed: %s\n", err);
        return;
    }

    if (!wait_for_exit) {
        resp->status = 0;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "started %s pid=%d rootfs=%s\n",
                 record->id,
                 record->host_pid,
                 record->rootfs);
        return;
    }

    while (1) {
        int done;

        reap_children(ctx);

        pthread_mutex_lock(&ctx->metadata_lock);
        done = !container_is_active(record);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (done)
            break;

        if (g_supervisor_stop)
            break;

        usleep(100000);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (record->state == CONTAINER_EXITED) {
        resp->status = record->exit_code;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "run complete: %s exited with code %d\n",
                 record->id,
                 record->exit_code);
    } else if (record->state == CONTAINER_HARD_LIMIT_KILLED) {
        resp->status = 128 + SIGKILL;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "run complete: %s killed due to hard limit (signal %d)\n",
                 record->id,
                 SIGKILL);
    } else if (record->exit_signal > 0) {
        resp->status = 128 + record->exit_signal;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "run complete: %s terminated by signal %d\n",
                 record->id,
                 record->exit_signal);
    } else {
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "run incomplete: %s has state %s\n",
                 record->id,
                 state_to_string(record->state));
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_stop(supervisor_ctx_t *ctx, const char *container_id, control_response_t *resp)
{
    container_record_t *record;
    pid_t target_pid;
    int active = 0;
    int waited_for_term = 0;
    int used_sigkill = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, container_id);
    if (record == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "unknown container id: %s\n", container_id);
        return;
    }

    if (!container_is_active(record)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "container %s is not running (state=%s)\n",
                 container_id,
                 state_to_string(record->state));
        return;
    }

    record->stop_requested = 1;
    target_pid = record->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(target_pid, SIGTERM) < 0 && errno != ESRCH) {
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "failed to stop %s (pid=%d): %s\n",
                 container_id,
                 target_pid,
                 strerror(errno));
        return;
    }

    /* Give the container a short graceful window and reap immediately if it exits. */
    for (int i = 0; i < 10; i++) {
        reap_children(ctx);
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id_locked(ctx, container_id);
        active = (record != NULL) ? container_is_active(record) : 0;
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!active)
            break;
        waited_for_term = 1;
        usleep(100000);
    }

    if (active) {
        used_sigkill = 1;
        if (kill(target_pid, SIGKILL) < 0 && errno != ESRCH) {
            resp->status = 1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "failed to force-stop %s (pid=%d): %s\n",
                     container_id,
                     target_pid,
                     strerror(errno));
            return;
        }

        for (int i = 0; i < 10; i++) {
            reap_children(ctx);
            pthread_mutex_lock(&ctx->metadata_lock);
            record = find_container_by_id_locked(ctx, container_id);
            active = (record != NULL) ? container_is_active(record) : 0;
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (!active)
                break;
            usleep(100000);
        }
    }

    resp->status = 0;
    if (!active && used_sigkill) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "force-stopped %s (pid=%d) with SIGKILL after SIGTERM timeout\n",
                 container_id,
                 target_pid);
    } else if (!active && waited_for_term) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "stopped %s (pid=%d)\n",
                 container_id,
                 target_pid);
    } else {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "stop signal sent to %s (pid=%d)\n",
                 container_id,
                 target_pid);
    }
}

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));

    if (read_all(client_fd, &req, sizeof(req)) != 0) {
        close(client_fd);
        return;
    }

    /* Keep metadata fresh for every client request. */
    reap_children(ctx);

    switch (req.kind) {
    case CMD_START:
        handle_start_or_run(ctx, &req, &resp, 0);
        break;
    case CMD_RUN:
        handle_start_or_run(ctx, &req, &resp, 1);
        break;
    case CMD_PS:
        fill_ps_response(ctx, &resp);
        break;
    case CMD_LOGS:
        fill_logs_response(ctx, req.container_id, &resp);
        break;
    case CMD_STOP:
        handle_stop(ctx, req.container_id, &resp);
        break;
    default:
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "unknown request kind: %d\n", req.kind);
        break;
    }

    (void)write_all(client_fd, &resp, sizeof(resp));
    close(client_fd);
}

static void *client_thread_main(void *arg)
{
    client_task_t *task = arg;

    handle_client(task->ctx, task->client_fd);
    free(task);
    return NULL;
}

static int running_container_count(supervisor_ctx_t *ctx)
{
    int count = 0;
    container_record_t *iter;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (container_is_active(iter))
            count++;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return count;
}

static void signal_running_containers(supervisor_ctx_t *ctx, int signo)
{
    container_record_t *iter;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (iter = ctx->containers; iter != NULL; iter = iter->next) {
        if (container_is_active(iter)) {
            if (signo == SIGTERM)
                iter->stop_requested = 1;
            (void)kill(iter->host_pid, signo);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void join_remaining_producers(supervisor_ctx_t *ctx)
{
    while (1) {
        pthread_t thread_to_join;
        int found = 0;
        container_record_t *iter;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (iter = ctx->containers; iter != NULL; iter = iter->next) {
            if (iter->producer_thread_started) {
                thread_to_join = iter->producer_thread;
                iter->producer_thread_started = 0;
                found = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!found)
            break;

        (void)pthread_join(thread_to_join, NULL);
    }
}

static void free_container_records(supervisor_ctx_t *ctx)
{
    container_record_t *iter;
    container_record_t *next;

    iter = ctx->containers;
    while (iter != NULL) {
        next = iter->next;
        if (iter->child_stack != NULL)
            free(iter->child_stack);
        free(iter);
        iter = next;
    }

    ctx->containers = NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct stat st;
    int rc;

    if (stat(rootfs, &st) < 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Base rootfs is not a directory: %s\n", rootfs);
        return 1;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (install_signal_handlers() != 0) {
        perror("install_signal_handlers");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_log_dir() != 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "Warning: /dev/container_monitor unavailable, continuing without monitor: %s\n",
                strerror(errno));
    }

    ctx.server_fd = create_control_socket();
    if (ctx.server_fd < 0) {
        perror("create_control_socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    printf("Supervisor started for base rootfs: %s\n", rootfs);
    printf("Control socket: %s\n", CONTROL_PATH);
    fflush(stdout);

    while (!g_supervisor_stop && !ctx.should_stop) {
        fd_set readfds;
        struct timeval tv;
        int ready;

        if (g_supervisor_child_event) {
            g_supervisor_child_event = 0;
            reap_children(&ctx);
        }

        reap_children(&ctx);

        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        ready = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (ready == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &readfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }

            client_task_t *task = calloc(1, sizeof(*task));
            pthread_t client_thread;

            if (task == NULL) {
                handle_client(&ctx, client_fd);
                continue;
            }

            task->ctx = &ctx;
            task->client_fd = client_fd;

            rc = pthread_create(&client_thread, NULL, client_thread_main, task);
            if (rc != 0) {
                free(task);
                handle_client(&ctx, client_fd);
                continue;
            }

            (void)pthread_detach(client_thread);
        }
    }

    signal_running_containers(&ctx, SIGTERM);
    for (int i = 0; i < 40; i++) {
        reap_children(&ctx);
        if (running_container_count(&ctx) == 0)
            break;
        usleep(100000);
    }

    if (running_container_count(&ctx) > 0) {
        signal_running_containers(&ctx, SIGKILL);
        for (int i = 0; i < 20; i++) {
            reap_children(&ctx);
            if (running_container_count(&ctx) == 0)
                break;
            usleep(100000);
        }
    }

    reap_children(&ctx);
    join_remaining_producers(&ctx);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    (void)pthread_join(ctx.logger_thread, NULL);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    free_container_records(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;
    int needs_run_signal_forwarding = (req->kind == CMD_RUN);

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Failed to connect to supervisor at %s: %s\n",
                CONTROL_PATH,
                strerror(errno));
        close(sock);
        return 1;
    }

    if (write_all(sock, req, sizeof(*req)) != 0) {
        perror("write request");
        close(sock);
        return 1;
    }

    memset(&resp, 0, sizeof(resp));
    if (needs_run_signal_forwarding) {
        struct sigaction sa;
        struct sigaction old_int;
        struct sigaction old_term;
        size_t received = 0;
        int forwarded = 0;
        int old_flags;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_client_signal_handler;

        if (sigaction(SIGINT, &sa, &old_int) < 0 ||
            sigaction(SIGTERM, &sa, &old_term) < 0) {
            perror("sigaction run client");
            close(sock);
            return 1;
        }

        g_run_client_signal = 0;

        old_flags = fcntl(sock, F_GETFL, 0);
        if (old_flags >= 0)
            (void)fcntl(sock, F_SETFL, old_flags | O_NONBLOCK);

        while (received < sizeof(resp)) {
            ssize_t n = read(sock,
                             ((char *)&resp) + received,
                             sizeof(resp) - received);
            if (n > 0) {
                received += (size_t)n;
                continue;
            }

            if (n == 0) {
                fprintf(stderr, "read response: unexpected EOF\n");
                (void)sigaction(SIGINT, &old_int, NULL);
                (void)sigaction(SIGTERM, &old_term, NULL);
                close(sock);
                return 1;
            }

            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                perror("read response");
                (void)sigaction(SIGINT, &old_int, NULL);
                (void)sigaction(SIGTERM, &old_term, NULL);
                close(sock);
                return 1;
            }

            if (g_run_client_signal != 0 && !forwarded) {
                control_request_t stop_req;
                memset(&stop_req, 0, sizeof(stop_req));
                stop_req.kind = CMD_STOP;
                copy_cstr(stop_req.container_id,
                          sizeof(stop_req.container_id),
                          req->container_id);
                (void)send_control_request(&stop_req);
                fprintf(stderr,
                        "run: forwarded signal %d as stop %s and still waiting for final status\n",
                        (int)g_run_client_signal,
                        req->container_id);
                forwarded = 1;
            }

            {
                fd_set readfds;
                struct timeval tv;
                int ready;

                FD_ZERO(&readfds);
                FD_SET(sock, &readfds);
                tv.tv_sec = 0;
                tv.tv_usec = 200000;

                ready = select(sock + 1, &readfds, NULL, NULL, &tv);
                if (ready < 0 && errno != EINTR) {
                    perror("select run response");
                    (void)sigaction(SIGINT, &old_int, NULL);
                    (void)sigaction(SIGTERM, &old_term, NULL);
                    close(sock);
                    return 1;
                }
            }
        }

        (void)sigaction(SIGINT, &old_int, NULL);
        (void)sigaction(SIGTERM, &old_term, NULL);
    } else {
        if (read_all(sock, &resp, sizeof(resp)) != 0) {
            perror("read response");
            close(sock);
            return 1;
        }
    }

    close(sock);

    if (resp.message[0] != '\0') {
        FILE *stream = (resp.status == 0 || req->kind == CMD_RUN) ? stdout : stderr;
        fputs(resp.message, stream);
    }

    if (req->kind == CMD_RUN)
        return resp.status;

    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
