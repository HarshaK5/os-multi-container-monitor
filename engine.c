/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
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
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
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
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
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
                int read_fd;
                char id[CONTAINER_ID_LEN];
                supervisor_ctx_t *ctx;
            } producer_arg_t;
            
void *producer_fn(void *arg);

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
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
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

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait while buffer is full
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    // If shutdown has started, stop pushing
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    // Insert item at tail
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    // Wake up one consumer
    pthread_cond_signal(&buffer->not_empty);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait while buffer is empty
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    // If empty AND shutdown → no more data
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    // Remove item from head
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    // Wake up one producer
    pthread_cond_signal(&buffer->not_full);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    // Ensure logs directory exists
    mkdir(LOG_DIR, 0755);

    while (1) {
        // Get next log item
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0) {
            // Shutdown + no more data
            break;
        }

        // Build log file path: logs/<container_id>.log
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("open log file");
            continue;
        }

        ssize_t written = write(fd, item.data, item.length);
        if (written < 0) {
            perror("write log");
        }

        close(fd);
    }

    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    // 1. Set hostname (UTS namespace)
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    // 2. Change root filesystem
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    // 3. Mount /proc inside container
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    // 4. Redirect stdout and stderr to logging pipe
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    close(cfg->log_write_fd);

    // 5. Set nice value (optional scheduling control)
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0) {
            perror("nice");
        }
    }

    // 6. Execute command
    execlp(cfg->command, cfg->command, NULL);

    // If exec fails
    perror("exec failed");
    return 1;
}

int register_with_monitor(int monitor_fd,
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
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

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

    // 1. Open kernel monitor device
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open monitor device");
        // not fatal for now
    }

    // 2. Setup UNIX domain socket
    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        return 1;
    }

    printf("Supervisor running...\n");

    // 3. Start logging thread
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create");
        return 1;
    }

    // 4. Main loop
    while (!ctx.should_stop) {

        // Reap children (non-blocking)
        int status;
        pid_t pid;

        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {

            pthread_mutex_lock(&ctx.metadata_lock);

            container_record_t *curr = ctx.containers;
            while (curr) {
                if (curr->host_pid == pid) {

                    if (WIFEXITED(status)) {
                        curr->state = CONTAINER_EXITED;
                        curr->exit_code = WEXITSTATUS(status);
                    } else if (WIFSIGNALED(status)) {
                        curr->state = CONTAINER_KILLED;
                        curr->exit_signal = WTERMSIG(status);
                    }

                    break;
                }
                curr = curr->next;
            }

            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        // Accept client request
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        control_request_t req;
        memset(&req, 0, sizeof(req));

        if (read(client_fd, &req, sizeof(req)) <= 0) {
            close(client_fd);
            continue;
        }

        // Handle START / RUN
        if (req.kind == CMD_START || req.kind == CMD_RUN) {

            int pipefd[2];
            if (pipe(pipefd) < 0) {
                perror("pipe");
                close(client_fd);
                continue;
            }

            child_config_t cfg;
            memset(&cfg, 0, sizeof(cfg));
            strncpy(cfg.id, req.container_id, sizeof(cfg.id) - 1);
            strncpy(cfg.rootfs, req.rootfs, sizeof(cfg.rootfs) - 1);
            strncpy(cfg.command, req.command, sizeof(cfg.command) - 1);
            cfg.nice_value = req.nice_value;
            cfg.log_write_fd = pipefd[1];

            void *stack = malloc(STACK_SIZE);
            if (!stack) {
              perror("malloc");
              close(client_fd);
              continue;
            }

            pid_t pid = clone(child_fn,
                              (char *)stack + STACK_SIZE,
                              CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                              &cfg);

            if (pid < 0) {
                perror("clone");
                close(client_fd);
                continue;
            }
            // 🔥 DEBUG PRINT
printf("DEBUG: clone successful, pid=%d\n", pid);

// 🔥 REGISTER WITH MONITOR
if (ctx.monitor_fd >= 0) {
    struct monitor_request mreq;
    memset(&mreq, 0, sizeof(mreq));

    mreq.pid = pid;
    mreq.soft_limit_bytes = req.soft_limit_bytes;
    mreq.hard_limit_bytes = req.hard_limit_bytes;

    strncpy(mreq.container_id, req.container_id, MONITOR_NAME_LEN - 1);
    mreq.container_id[MONITOR_NAME_LEN - 1] = '\0';

    printf("DEBUG: registering container %s\n", mreq.container_id);

    if (ioctl(ctx.monitor_fd, MONITOR_REGISTER, &mreq) < 0) {
        perror("monitor register ioctl FAILED");
    } else {
        printf("DEBUG: monitor registration SUCCESS\n");
    }
} else {
    printf("DEBUG: monitor_fd is invalid\n");
}

            close(pipefd[1]); // parent doesn't write

            // Save metadata
            container_record_t *rec = malloc(sizeof(container_record_t));
            memset(rec, 0, sizeof(*rec));

            strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
            rec->host_pid = pid;
            rec->started_at = time(NULL);
            rec->state = CONTAINER_RUNNING;

            pthread_mutex_lock(&ctx.metadata_lock);
            rec->next = ctx.containers;
            ctx.containers = rec;
            pthread_mutex_unlock(&ctx.metadata_lock);

            producer_arg_t *parg = malloc(sizeof(producer_arg_t));
            if (!parg) {
              perror("malloc");
              close(pipefd[0]);
              close(client_fd);
              continue;
            }
            parg->read_fd = pipefd[0];
            strncpy(parg->id, req.container_id, CONTAINER_ID_LEN - 1);
            parg->ctx = &ctx;

            pthread_t producer;
            pthread_create(&producer, NULL, producer_fn, parg);
            pthread_detach(producer);

            if (req.kind == CMD_RUN) {
                waitpid(pid, NULL, 0);
            }
        }

        else if (req.kind == CMD_PS) {

    char response[CONTROL_MESSAGE_LEN];
    memset(response, 0, sizeof(response));

    pthread_mutex_lock(&ctx.metadata_lock);

    container_record_t *curr = ctx.containers;
    int offset = 0;

    if (!curr) {
        snprintf(response, sizeof(response), "No containers running\n");
    } else {
        while (curr && offset < (int)sizeof(response) - 64) {
            offset += snprintf(response + offset,
                               sizeof(response) - offset,
                               "ID=%s PID=%d STATE=%s\n",
                               curr->id,
                               curr->host_pid,
                               state_to_string(curr->state));
            curr = curr->next;
        }
    }

    pthread_mutex_unlock(&ctx.metadata_lock);

    // Send response back to client
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.status = 0;
    strncpy(resp.message, response, sizeof(resp.message) - 1);

    write(client_fd, &resp, sizeof(resp));
}

        else if (req.kind == CMD_STOP) {
            pthread_mutex_lock(&ctx.metadata_lock);

            container_record_t *curr = ctx.containers;
            while (curr) {
                if (strcmp(curr->id, req.container_id) == 0) {
                    kill(curr->host_pid, SIGTERM);
                    curr->state = CONTAINER_STOPPED;
                    break;
                }
                curr = curr->next;
            }

            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        close(client_fd);
    }

    // Shutdown
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    return 0;
}

void *producer_fn(void *arg) {
                producer_arg_t *p = arg;
                char buf[LOG_CHUNK_SIZE];
                ssize_t n;

                while ((n = read(p->read_fd, buf, sizeof(buf))) > 0) {
                    log_item_t item;
                    memset(&item, 0, sizeof(item));

                    strncpy(item.container_id, p->id, CONTAINER_ID_LEN - 1);
                    memcpy(item.data, buf, n);
                    item.length = n;

                    bounded_buffer_push(&p->ctx->log_buffer, &item);
                }

                close(p->read_fd);
                free(p);
                return NULL;
            }

static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;

    // 1. Create socket
    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    // 2. Setup address
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    // 3. Connect to supervisor
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is supervisor running?)");
        close(sock);
        return 1;
    }

    // 4. Send request
    if (write(sock, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(sock);
        return 1;
    }

    // 5. Optional: receive response
    control_response_t resp;
    ssize_t n = read(sock, &resp, sizeof(resp));

    if (n > 0) {
        printf("%s\n", resp.message);
    }

    close(sock);
    return 0;
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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
