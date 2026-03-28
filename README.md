# Multi-Container Runtime with Kernel Memory Monitor

---

## Team Information

### Member 1
* **Name:** Harsha K
* **SRN:** *PES1UG24CS184*


### Member 2
* **Name:** Chennamraju Vaishnavi
* **SRN:** *PES1UG24CS129*
  
---

## Project Summary

This project implements a lightweight Linux container runtime in C, featuring:

* A long-running supervisor process for managing containers
* A kernel module (LKM) for enforcing memory limits
* A multi-container execution environment
* A bounded-buffer logging system
* A CLI interface with IPC communication
* Scheduling experiments to study Linux behavior

The system demonstrates core OS concepts including namespaces, IPC, process lifecycle, synchronization, memory management, and scheduling.

---

## Environment Setup

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

Ensure:

* Ubuntu 22.04/24.04 VM
* Secure Boot OFF
* Not running on WSL

---

## Build Instructions

```bash
make
```

---

## Load Kernel Module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

---

## Running the System

### 1. Prepare Root Filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

Create per-container rootfs:

```bash
cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
```

---

### 2. Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

---

### 3. Start Containers

```bash
sudo ./engine start alpha ./rootfs-alpha /memory_hog --soft-mib 5 --hard-mib 10
sudo ./engine start beta ./rootfs-beta /memory_hog --soft-mib 15 --hard-mib 25
```

---

### 4. List Containers

```bash
sudo ./engine ps
```

---

### 5. View Logs

```bash
sudo ./engine logs alpha
```

---

### 6. Stop Containers

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

---

### 7. Kernel Logs

```bash
dmesg | tail
```

---

### 8. Cleanup

```bash
sudo pkill engine
sudo rmmod monitor
make clean
```

---

## CLI Contract

```bash
engine supervisor <base-rootfs>
engine start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]
engine run   <id> <rootfs> <cmd> ...
engine ps
engine logs <id>
engine stop <id>
```

---

## Architecture Overview

### Supervisor

* Long-running parent process
* Handles container lifecycle
* Maintains metadata
* Manages logging and IPC

### CLI Client

* Sends commands via IPC
* Short-lived process

### Kernel Module

* Tracks container PIDs
* Enforces memory limits
* Logs violations

---

## IPC Design

### Path A — Logging

* Uses pipes
* Container → Supervisor
* Captures stdout/stderr

### Path B — Control

* Uses UNIX domain socket / FIFO
* CLI → Supervisor

### Why two IPC mechanisms?

* Logging = continuous stream
* Control = discrete commands

---

## Bounded Buffer Logging

### Design

* **Producer threads**

  * Read container output
  * Push into buffer

* **Consumer threads**

  * Write logs to file

### Synchronization

* Mutex for mutual exclusion
* Condition variables for:

  * Buffer full
  * Buffer empty

### Guarantees

* No data loss
* No deadlocks
* Clean shutdown

---

## Kernel Memory Monitoring

### Features

* `/dev/container_monitor` device
* `ioctl` for PID registration
* Linked list of tracked processes

### Policies

* **Soft Limit**

  * Logs warning

* **Hard Limit**

  * Sends `SIGKILL`

### Example Output

```text
SOFT LIMIT container=alpha ...
HARD LIMIT container=alpha ...
```

---

## Scheduling Experiments

### Experiment 1: CPU vs CPU

```bash
nice -n 5 ./cpu_hog
nice -n 10 ./cpu_hog
```

**Observation:**
Lower nice value → more CPU share

---

### Experiment 2: CPU vs IO

* CPU-bound: `cpu_hog`
* IO-bound: `io_pulse`

**Observation:**
IO tasks remain responsive while CPU tasks dominate processing time

---

## Demo Screenshots

*(Add images here with captions)*

1. Multi-container running
2. `engine ps` output
3. Logs file
4. CLI interaction
5. Soft limit log
6. Hard limit kill
7. Scheduling experiment
8. Clean shutdown

---

## Resource Cleanup

* No zombie processes (`waitpid`)
* Threads exit cleanly
* File descriptors closed
* Kernel list freed on unload

---

## Engineering Analysis

### 1. Isolation Mechanisms

* PID namespace → process isolation
* UTS namespace → hostname isolation
* Mount namespace + chroot → filesystem isolation

Host kernel is shared across containers.

---

### 2. Supervisor & Lifecycle

* Parent process manages children
* Handles `SIGCHLD`
* Tracks metadata
* Prevents zombie processes

---

### 3. IPC & Synchronization

* Pipes for logging
* Socket/FIFO for control
* Mutex + condition variables prevent race conditions

Without synchronization:

* Data corruption
* Lost logs
* Deadlocks

---

### 4. Memory Management

* RSS = physical memory usage
* Soft limit = warning
* Hard limit = enforcement

Kernel-level enforcement ensures reliability.

---

### 5. Scheduling Behavior

* Linux uses CFS scheduler
* Nice values influence priority
* Observed fairness vs responsiveness tradeoff

---

## Design Decisions & Tradeoffs

| Component      | Choice         | Tradeoff                    |
| -------------- | -------------- | --------------------------- |
| Filesystem     | chroot         | Less secure than pivot_root |
| IPC            | pipes + socket | Increased complexity        |
| Logging        | threads        | Synchronization overhead    |
| Kernel monitor | LKM            | Requires root privileges    |

---

## Screenshots of Outputs

### Multi-container Supervision
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/dc7a48bb-61e8-41e7-bed9-d7e98c7b9ed2" />
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/46c53c18-50df-470d-b5b1-176a211e9513" />

### Metadata Tracking
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/dca18ebf-11a8-4b9c-81f0-cb266787f47c" />

### Logging Pipeline
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/1ac4e095-8e6a-4e81-a385-31e8d270c8e5" />

### CLI + IPC
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/f8b1bc60-2ca9-4314-9aca-20b3675f0704" />

### SOFT LIMIT
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/6ce0319a-c4c4-4a24-85ea-1911bc94a3fa" />
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/7273644e-fea7-4c96-ac6c-be10b5a5cf5b" />

### HARD LIMIT
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/60d20ab6-1033-4d93-9457-26397ebd5309" />

### Scheduling Experiment
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/038d5056-f3f9-419f-8b9d-f0feb6400023" />
Process 1 took only about 3 seconds and process 2 took about 8 seconds

### Clean Teardown
<img width="1854" height="890" alt="image" src="https://github.com/user-attachments/assets/59236889-64d2-45aa-907f-ad3aa05190b3" />

---

## Repository Contents

* `engine.c`
* `monitor.c`
* `monitor_ioctl.h`
* `cpu_hog.c`, `memory_hog.c`, `io_pulse.c`
* `Makefile`
* `README.md`

---

## Results and Observations

### Multi-container Supervision

The system successfully launched and managed multiple containers (`alpha`, `beta`) simultaneously under a single supervisor process.

**Observation:**

* Both containers were created and executed concurrently.
* The supervisor maintained control over all running containers without conflict.
* Demonstrates correct implementation of multi-container orchestration.

---

### Metadata Tracking

The `engine ps` command correctly displayed container metadata including ID, PID, and execution state.

**Observation:**

* Container `beta` remained in `running` state.
* Container `alpha` transitioned to `killed` state after exceeding limits.
* Confirms accurate lifecycle tracking and state management.

---

### Logging Pipeline

Container output was successfully captured and written to log files.

**Observation:**

* Logs show continuous memory allocation output from `memory_hog`.
* No missing or corrupted entries.
* Confirms correct implementation of producer-consumer logging with synchronization.

---

### CLI + IPC

The CLI (`engine`) communicated effectively with the supervisor using IPC mechanisms.

**Observation:**

* Commands like `start`, `ps`, and `logs` executed correctly.
* Supervisor responded reliably to client requests.
* Demonstrates proper separation between control plane and execution layer.

---

### SOFT LIMIT Enforcement

Soft memory limit violations were detected and logged by the kernel module.

**Observation:**

* Kernel logs show `SOFT LIMIT` messages when memory usage exceeded threshold.
* Containers were not terminated at this stage.
* Confirms warning-based enforcement policy.

---

### HARD LIMIT Enforcement

Hard memory limit violations resulted in immediate container termination.

**Observation:**

* Kernel logs show `HARD LIMIT` messages followed by process termination.
* Container `alpha` was killed after exceeding its hard limit.
* Confirms strict enforcement using kernel-level control (SIGKILL).

---

### Scheduling Experiment

Two processes with different priorities were executed to observe scheduling behavior.

**Observation:**

* Process 1 completed in ~3 seconds.
* Process 2 completed in ~8 seconds.
* Lower nice value (higher priority) resulted in faster execution.
* Demonstrates behavior of Linux Completely Fair Scheduler (CFS).

---

### Clean Teardown

System resources were properly released after execution.

**Observation:**

* All containers were stopped successfully.
* No zombie processes remained.
* Kernel module unloaded cleanly.
* Temporary files and sockets were removed.

---

## Final Outcome

The system successfully demonstrates:

* Multi-container lifecycle management
* Kernel-level memory monitoring and enforcement
* Reliable logging using synchronized buffering
* Effective IPC between CLI and supervisor
* Real-world scheduling behavior
* Proper resource cleanup

All components function cohesively, validating the correctness of the implementation.

