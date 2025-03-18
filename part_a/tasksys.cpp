#include "tasksys.h"


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
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->threads = std::vector<std::thread>(num_threads);
    this->counter = 0;
    this->mutex = new std::mutex();
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) 
{
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->threads.clear();
    for(int i = 0; i < num_total_tasks; i++)
    {
        this->mutex->lock();
        this->threads.emplace_back([=]() { runnable->runTask(i, num_total_tasks); });
        this->mutex->unlock();
    }
    for(int i = 0; i < num_total_tasks; i++)
        this->threads[i].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::spinFunc(IRunnable* runnable, int num_total_tasks, lockylock& lock, int& nextTask) {
    int curTask = -1;
    while (curTask < num_total_tasks) {
        lock.lock();
        curTask = nextTask;
        nextTask++;
        lock.unlock();
        runnable->runTask(curTask, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    lockylock spinlock;
    int nextTask = 0;
    for (int i = 0; i < this->num_threads; i++) {
        pool.emplace_back(std::thread(&TaskSystemParallelThreadPoolSpinning::spinFunc, this, runnable, num_total_tasks, std::ref(spinlock), std::ref(nextTask)));
    }    
    for(int i = 0; i < this->num_threads; i++)
        pool[i].join();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = false;
    totalTask = finishedTask = 0;
    this->num_threads = num_threads;
    for (int i = 0; i < num_threads; i++){
        this->pool.emplace_back([this] { worker_thread(); });
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    while (true) {
        std::unique_lock<std::mutex> lockConsumer(mutexConsumer);
        auto func = [this]() { return stop || (nextTask < totalTask); };
        cvConsumer.wait(lockConsumer, func);

        if (stop && nextTask == totalTask) {
            // std::cerr << "worker quit" << std::endl;
            break;
        }

        int taskIndex = nextTask++;
        lockConsumer.unlock();
        // std::cerr << "worker run: " << taskIndex << std::endl;
        runner->runTask(taskIndex, totalTask);
        {
            std::lock_guard<std::mutex> lockFinish(mutexFinish);
            finishedTask++;
            // std::cerr << "finished task: " << finishedTask << std::endl;
            if (finishedTask == totalTask) {
                cvProducer.notify_all();
            }
        }
    }
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = true;
    cvConsumer.notify_all();
    for (int i = 0; i < this->num_threads; i++) {
        pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    totalTask = num_total_tasks;
    finishedTask = 0;
    {
        std::lock_guard<std::mutex> lockConsumer(mutexConsumer);
        nextTask = 0;
    }
    // std::cerr << "producer notify all consumer" << std::endl;
    cvConsumer.notify_all();

    std::unique_lock<std::mutex> lockFinish(mutexFinish);
    auto func = [this]() { return finishedTask == totalTask; };
    cvProducer.wait(lockFinish, func);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
