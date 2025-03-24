//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <vector>
#include <bits/stdc++.h>
#include <unordered_map>

vector<vector<VMId_t>> machineToVM;
unordered_map<VMId_t, MachineId_t> migrationMap;
unordered_map<TaskId_t, VMId_t> taskToVM;

unsigned tasks_per_cpu = 2;

unsigned VMSize(VMId_t vm_id);
unsigned estimatedMemoryAvailable(MachineId_t machine_id);
unsigned estimatedActiveTasks(MachineId_t machine_id);
unsigned estimatedAvailableTasks(MachineId_t machine_id);


void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        machines.push_back(MachineId_t(i));
        vector<VMId_t> MTV;
        machineToVM.push_back(MTV);
    }    


    // SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks

    MachineId_t source_id = migrationMap[vm_id];

    for(unsigned i = 0; i < machineToVM[source_id].size(); i++){
        if(machineToVM[source_id][i] == vm_id){
            machineToVM[source_id].erase(machineToVM[source_id].begin() + i);
            break;
        }
    }

    migrationMap.erase(vm_id);

    VMInfo_t currentVM = VM_GetInfo(vm_id);
    if(currentVM.active_tasks.size() == 0){
        for(unsigned i = 0; i < machineToVM[currentVM.machine_id].size(); i++){
            if(machineToVM[currentVM.machine_id][i] == currentVM.vm_id){
                machineToVM[currentVM.machine_id].erase(machineToVM[currentVM.machine_id].begin() + i);
                VM_Shutdown(currentVM.vm_id);
                break;
            }
        }
    }

    
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired

    TaskInfo_t task = GetTaskInfo(task_id);
    bool task_added = false;
    // cout <<"adding task " << task_id << endl;
    for(unsigned i = 0; i < machines.size(); i++)
    {
        MachineInfo_t currentMachine = Machine_GetInfo(machines[i]);

        if(currentMachine.cpu != task.required_cpu){
            continue;
        }

        if(estimatedMemoryAvailable(currentMachine.machine_id) != currentMachine.memory_size - currentMachine.memory_used){
            // cout << "Machine " << currentMachine.machine_id << " estimated memory available: " << estimatedMemoryAvailable(currentMachine.machine_id) << " actual: " << currentMachine.memory_size - currentMachine.memory_used << endl;
        }
        if(estimatedActiveTasks(currentMachine.machine_id) != currentMachine.active_tasks){
            // cout << "estimatedActiveTasks(currentMachine.machine_id) != currentMachine.active_tasks" << endl;
        }

        if(estimatedMemoryAvailable(currentMachine.machine_id) > (task.required_memory + 8) && currentMachine.num_cpus * tasks_per_cpu - estimatedActiveTasks(currentMachine.machine_id) > 0){

            for(unsigned j = 0; j < machineToVM[i].size(); j++){
                VMInfo_t currentVM = VM_GetInfo(machineToVM[i][j]);
                if(currentVM.vm_type == task.required_vm && !migrationMap.count(machineToVM[i][j])){
                    VM_AddTask(currentVM.vm_id, task.task_id, MID_PRIORITY);
                    taskToVM[task.task_id] = currentVM.vm_id;
                    task_added = true;
                    break;
                }
            }

            if(!task_added){
                VMId_t newVM = VM_Create(task.required_vm, task.required_cpu);
                VM_Attach(newVM, currentMachine.machine_id);
                VM_AddTask(newVM, task.task_id, MID_PRIORITY);
                machineToVM[i].push_back(newVM);
                taskToVM[task.task_id] = newVM;
                task_added = true;
            }
            break;

        }
        else if (estimatedMemoryAvailable(currentMachine.machine_id) > task.required_memory && currentMachine.num_cpus * tasks_per_cpu - estimatedActiveTasks(currentMachine.machine_id) > 0){

            for(unsigned j = 0; j < machineToVM[i].size(); j++){
                VMInfo_t currentVM = VM_GetInfo(machineToVM[i][j]);
                if(currentVM.vm_type == task.required_vm && !migrationMap.count(machineToVM[i][j])){
                    VM_AddTask(currentVM.vm_id, task.task_id, MID_PRIORITY);
                    taskToVM[task.task_id] = currentVM.vm_id;
                    task_added = true;
                    break;
                }
            }
            break;

        }


    }

    if(!task_added){
        // cout << "task_added " << task_id << " " << task_added << endl;
    }

    
    
    // Skeleton code, you need to change it according to your algorithm
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        // cout << "shutting down " << vm <<endl;
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

    cout << "Task completed " << task_id << endl;

    //if VM empty DESTROY!!!!
    VMInfo_t completedTaskVM = VM_GetInfo(taskToVM[task_id]);
    //MIGRATION CHECK?????
    if(completedTaskVM.active_tasks.size() == 0 && !migrationMap.count(taskToVM[task_id])){

        for(unsigned i = 0; i < machineToVM[completedTaskVM.machine_id].size(); i++){
            if(machineToVM[completedTaskVM.machine_id][i] == completedTaskVM.vm_id){
                machineToVM[completedTaskVM.machine_id].erase(machineToVM[completedTaskVM.machine_id].begin() + i);
                // cout << "shutting down VM " << completedTaskVM.vm_id << " on machine " << completedTaskVM.machine_id << endl;
                VM_Shutdown(completedTaskVM.vm_id);
                break;
            }
        }
        
    }

    taskToVM.erase(task_id);

    vector<MachineId_t> machinesCopy = machines;
    unordered_map<VMId_t, MachineId_t> destinationMap;

    // sorts in ascending order by utilization measured as active tasks
    sort(machinesCopy.begin(), machinesCopy.end(), [](MachineId_t a, MachineId_t b) {return estimatedActiveTasks(a) * Machine_GetInfo(b).num_cpus <  estimatedActiveTasks(b) * Machine_GetInfo(a).num_cpus; });

    // cout << "Machines" << endl;
    // for (auto i : machines)
    //     cout << i << " ";
    // cout << endl;
    // cout << "machinesCopy" << endl;
    // for (auto i : machinesCopy)
    //     cout << i << " ";
    // cout << endl;
    // cout << "machinesCopy active tasks" << endl;
    // for (auto i : machinesCopy)
    //     cout << Machine_GetInfo(i).active_tasks << " ";
    // cout << endl;

    for(unsigned sourceMachine_index = 0; sourceMachine_index < machinesCopy.size(); sourceMachine_index++){
        MachineId_t sourceMachine_id = machinesCopy[sourceMachine_index];
        MachineInfo_t sourceMachine = Machine_GetInfo(sourceMachine_id);

        if(sourceMachine.active_tasks < 1){
            continue;
        }

        for(unsigned sourceVM_index = 0; sourceVM_index < machineToVM[sourceMachine_id].size(); sourceVM_index++){
            VMId_t sourceVM_id = machineToVM[sourceMachine_id][sourceVM_index];
            VMInfo_t sourceVM = VM_GetInfo(sourceVM_id);

            for(unsigned destMachine_index = sourceMachine_index + 1; destMachine_index < machinesCopy.size(); destMachine_index++){
                MachineId_t destMachine_id = machinesCopy[destMachine_index];
                MachineInfo_t destMachine = Machine_GetInfo(destMachine_id);
    
                if(estimatedAvailableTasks(destMachine_id) >= sourceVM.active_tasks.size() && estimatedMemoryAvailable(destMachine_id) >= VMSize(sourceVM_id) && !migrationMap.count(sourceVM_id) && sourceVM.cpu == destMachine.cpu){
                    //cout << "VM id " << sourceVM_id << " sourceVM.machine_id: " << sourceVM.machine_id << " sourceMachine_id " << sourceMachine_id << endl;
                    if(sourceVM.machine_id != sourceMachine_id){
                        // cout << "sourceVM.machine_id: " << sourceVM.machine_id << " sourceMachine_id " << sourceMachine_id << endl;

                        MachineId_t prev_dest = destinationMap[sourceVM.vm_id];
                        // cout << "prev_dest: " << prev_dest << endl;
                        bool deleted = false;
                        for(unsigned i = 0; i < machineToVM[prev_dest].size(); i++){
                            if(machineToVM[prev_dest][i] == sourceVM_id){
                                machineToVM[prev_dest].erase(machineToVM[prev_dest].begin() + i);
                                deleted = true;
                                break;
                            }
                        }

                        if(!deleted){
                            cout << "failed to remove VM: " << sourceVM.vm_id << " from prev_dest: " << prev_dest << endl;
                        }                    
                    }

                    //fake migrate
                    // cout << "fake migrating VM: " << sourceVM_id << " to machine: " << destMachine_id << endl;
                    destinationMap[sourceVM_id] = destMachine_id;
                    machineToVM[destMachine_id].push_back(sourceVM_id);
                    break; //HAVE IAN APPROVE THIS
                }
    
    
            }


        }

    }

    //migrate all in destination map
    //update migration map
    for(auto it = destinationMap.begin(); it != destinationMap.end(); it++){
        // cout << "real migrating " << it->first << " to " << it->second << endl;
        migrationMap[it->first] = VM_GetInfo(it->first).machine_id;
        VM_Migrate(it->first, it->second);
    }


    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

unsigned estimatedMemoryAvailable(MachineId_t machine_id){
    MachineInfo_t machine = Machine_GetInfo(machine_id);

    unsigned mem_used = 0;
    for(unsigned i = 0; i < machineToVM[machine_id].size(); i++){
        mem_used += VMSize(machineToVM[machine_id][i]);
    }
    if(mem_used > machine.memory_size){
        cout << "issue in estimatedMemoryAvailable(), mem_used > machine.memory_size" << endl;
    }
    return machine.memory_size - mem_used;
}

unsigned VMSize(VMId_t vm_id){
    unsigned mem_used = 8;
    VMInfo_t currentVM = VM_GetInfo(vm_id);
    for(unsigned j = 0 ; j < currentVM.active_tasks.size(); j++){
        mem_used += GetTaskInfo(currentVM.active_tasks[j]).required_memory;
    }

    return mem_used;

}

unsigned estimatedActiveTasks(MachineId_t machine_id){
    MachineInfo_t machine = Machine_GetInfo(machine_id);

    unsigned tasks = 0;
    for(unsigned i = 0; i < machineToVM[machine_id].size(); i++){
        tasks += VM_GetInfo(machineToVM[machine_id][i]).active_tasks.size();
    }
    return tasks;
}

unsigned estimatedAvailableTasks(MachineId_t machine_id){
    MachineInfo_t machine = Machine_GetInfo(machine_id);

    unsigned est_active = estimatedActiveTasks(machine_id);
    return machine.num_cpus * tasks_per_cpu - est_active;
}