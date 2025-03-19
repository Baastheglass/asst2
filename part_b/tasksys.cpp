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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    t_manager = new TaskManager();
    current_id = 0;
    num_of_threads = num_threads;
    work_exists = true;
    thread_pool = new std::thread[num_threads];
    for(int i = 0; i<num_threads; i++){
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::worker_thread, this);
    } 
}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    work_exists = false;
    {
        std::lock_guard<std::mutex> lock(*(t_manager->executing_mutex));
        t_manager->sleeping_thread->notify_all();
    }
    for(int i = 0; i<num_of_threads; i++){
        thread_pool[i].join();
    }
    delete[] thread_pool;
    delete  t_manager;
}
/* run - is a single job
   working_rn - keeps track of the state of all running runs
   local_elem - the piece of work the thread is currently doing
   executing_vec - work free for the picking
   waiting_vec - vec which has dependencies that its waiting on.

 */
 void TaskSystemParallelThreadPoolSleeping::worker_thread(){
    Task_Element local_elem;
    bool not_skip = true;
    while(work_exists){
        if(not_skip){
            std::unique_lock<std::mutex> lock3(*(thread_manager->executing_mutex));
            local_elem=thread_manager->fetch_work();
            if(local_elem.myID<0){
                if(work_exists){
                    thread_manager->sleeping_thread->wait(lock3);
                    local_elem=thread_manager->fetch_work();
                }
            }
            lock3.unlock();
        }
        if(local_elem.myID >= 0){
            not_skip = true;
            local_elem.runnable->runTask(local_elem.count,local_elem.num_total_tasks);
            thread_manager->task_finished->lock();
            auto it = thread_manager->working_rn.find(local_elem.myID);
            if (it == thread_manager->working_rn.end()) {
                thread_manager->working_rn[local_elem.myID] = 1;
            }
            else{
                thread_manager->working_rn[local_elem.myID] += 1;
            }
            if(thread_manager->working_rn[local_elem.myID] == local_elem.num_total_tasks){
                thread_manager->working_rn.erase(local_elem.myID);
                thread_manager->task_finished->unlock();
                thread_manager->run_finished->lock();

                thread_manager->runs_finished.insert(local_elem.myID);
                thread_manager->latest_finished++;
                if(thread_manager->latest_finished+1 == curr_run_id){
                    thread_manager->finishing->notify_all();
                    thread_manager->run_finished->unlock();
                }
                else{
                    thread_manager->run_finished->unlock();
                }

                std::unique_lock<std::mutex> lock2(*thread_manager->waiting_mutex);
                auto it = thread_manager->dependency_map.find(local_elem.myID);
                if (it !=  thread_manager->dependency_map.end()) {
                    bool new_elem = false;
                    for (TaskID dependent : it->second) {
                        thread_manager->dependency_count[dependent]--;
                        if (thread_manager->dependency_count[dependent] == 0) {
                            if(new_elem==false){
                                thread_manager->executing_mutex->lock();
                                new_elem = true;
                            }
                            thread_manager->executing_vec.push(thread_manager->waiting_vec[dependent]);
                            thread_manager->waiting_vec.erase(dependent);
                        }
                    }
                    if(new_elem){
                        local_elem=thread_manager->fetch_work();
                        not_skip = false;
                        thread_manager->executing_mutex->unlock();
                        thread_manager->sleeping_thread->notify_all();
                    }
                    thread_manager->dependency_map.erase(it->first);
                    lock2.unlock();
                }
            }
            else{
                thread_manager->task_finished->unlock();
            }
        }
    }
}
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    t_manager->run_finished->lock();
    bool not_found = true;
    int deps_size = 0;
    for(TaskID dep : deps) {
        if(t_manager->runs_finished.find(dep)==t_manager->runs_finished.end()){
            if(not_found){
                t_manager->waiting_mutex->lock();
                not_found = false;
            }
            t_manager->dependency_map[dep].insert(curr_run_id);
            deps_size++;
        }
    }

    curr_run_id++;
    t_manager->run_finished->unlock();
    if(not_found){
        t_manager->executing_mutex->lock();
        t_manager->executing_vec.push(Task_Element(runnable,num_total_tasks,0,curr_run_id-1));
        // wake up sleeping threads
        t_manager->executing_mutex->unlock();
        t_manager->sleeping_thread->notify_all();
    }
    else{
        t_manager->dependency_count[curr_run_id-1] = deps_size;
        t_manager->waiting_vec[curr_run_id-1] = Task_Element(runnable,num_total_tasks,0,curr_run_id-1);
        t_manager->waiting_mutex->unlock();
    }
    return curr_run_id-1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(*(t_manager->run_finished));
    if(t_manager->latest_finished + 1 == curr_run_id){
        return;
    }
    else
    {
        t_manager->finishing->wait(lock);
    }
    return;
}
