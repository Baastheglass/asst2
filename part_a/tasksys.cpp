#include "tasksys.h"

// Base class implementation
IRunnable::~IRunnable() {}
ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
    // No initialization needed for serial implementation
}

TaskSystemSerial::~TaskSystemSerial() {
    // No cleanup needed for serial implementation
}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    // Execute tasks sequentially
    int task_id = 0;
    while (task_id < num_total_tasks) {
        runnable->runTask(task_id, num_total_tasks);
        task_id++;
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // Not required for implementation
    return 0;
}

void TaskSystemSerial::sync() {
    // Not required for implementation
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // Initialize thread pool variables
    this->num_threads = num_threads;
    this->threads.reserve(num_threads);
    this->counter = 0;
    this->mutex = new std::mutex();
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    // Clean up allocated resources
    delete this->mutex;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Clear any previous threads
    this->threads.clear();
    
    // Create a new thread for each task
    for (int task_id = 0; task_id < num_total_tasks; task_id++) {
        std::lock_guard<std::mutex> guard(*this->mutex);
        this->threads.push_back(std::thread([runnable, task_id, num_total_tasks]() {
            runnable->runTask(task_id, num_total_tasks);
        }));
    }
    
    // Wait for all threads to complete
    for (auto& thread : this->threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // Not required for implementation
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // Not required for implementation
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // Store the number of threads for use in run()
    this->num_threads = num_threads;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // No explicit cleanup required
}

void TaskSystemParallelThreadPoolSpinning::spinFunc(IRunnable* runnable, int num_total_tasks, lockylock& lock, int& nextTask) {
    // Worker function for each thread
    while (true) {
        // Acquire lock and get the next task
        lock.lock();
        int currentTask = nextTask++;
        lock.unlock();
        
        // Check if we're done with all tasks
        if (currentTask >= num_total_tasks) {
            break;
        }
        
        // Execute the task
        runnable->runTask(currentTask, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Create synchronization objects
    lockylock taskLock;
    int nextTaskId = 0;
    
    // Clear any previous threads
    pool.clear();
    
    // Create thread pool
    for (int i = 0; i < this->num_threads; i++) {
        pool.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::spinFunc, 
                                  this, runnable, num_total_tasks, 
                                  std::ref(taskLock), std::ref(nextTaskId)));
    }
    
    // Wait for all threads to complete
    for (auto& thread : pool) {
        thread.join();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // Not required for implementation
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // Not required for implementation
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    // Initialize thread pool variables
    this->num_threads = num_threads;
    this->stop = false;
    this->totalTask = 0;
    this->finishedTask = 0;
    this->nextTask = 0;
    
    // Create and start worker threads
    for (int i = 0; i < num_threads; i++) {
        pool.emplace_back([this] { this->worker_thread(); });
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    // Worker thread function
    while (true) {
        int taskToExecute;
        
        // Wait for work or termination signal
        {
            std::unique_lock<std::mutex> lock(mutexConsumer);
            cvConsumer.wait(lock, [this] { 
                return this->stop || (this->nextTask < this->totalTask); 
            });
            
            // Check if we should terminate
            if (this->stop && this->nextTask >= this->totalTask) {
                break;
            }
            
            // Get the next task
            taskToExecute = this->nextTask++;
        }
        
        // Execute the task
        this->runner->runTask(taskToExecute, this->totalTask);
        
        // Mark task as completed
        {
            std::lock_guard<std::mutex> lock(mutexFinish);
            this->finishedTask++;
            
            // If all tasks are done, notify the main thread
            if (this->finishedTask == this->totalTask) {
                cvProducer.notify_all();
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Signal all threads to terminate
    {
        std::lock_guard<std::mutex> lock(mutexConsumer);
        this->stop = true;
    }
    
    // Wake up all threads so they can check the stop flag
    cvConsumer.notify_all();
    
    // Wait for all threads to finish
    for (auto& thread : pool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(int num_total_tasks) {
    // Set up task parameters
    this->totalTask = num_total_tasks;
    this->finishedTask = 0;
    
    // Reset task counter
    {
        std::lock_guard<std::mutex> lock(mutexConsumer);
        this->nextTask = 0;
    }
    
    // Wake up worker threads
    cvConsumer.notify_all();
    
    // Wait for all tasks to complete
    {
        std::unique_lock<std::mutex> lock(mutexFinish);
        cvProducer.wait(lock, [this] { 
            return this->finishedTask == this->totalTask; 
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // Not required for this implementation
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // Not required for this implementation
}