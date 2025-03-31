//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#include "Scheduler.hpp"
#include "Interfaces.h"
#include "SimTypes.h"
#include <vector>
#include <queue>
#include <unordered_map>
#include <bits/stdc++.h>

unsigned migrating = 0;

void addTask(TaskId_t task_id);
void retryUnaddedTasks();
void migrate();

typedef struct VMData{
    unsigned task_count;
    unsigned mem_size;
    MachineId_t destination;
    MachineId_t source;
    bool migrating;
} VMData;

typedef struct MachineData{
    unsigned mem_available;
    unsigned max_tasks;
    unsigned task_count;
} MachineData;

queue<TaskId_t> unadded_tasks;

//CPU Type, GPU? ==> MachineId
unordered_map<CPUType_t, unordered_map<bool,vector<MachineId_t>>> CPUtoMachine;
//CPU Type, GPU?, VM Type ==> VM Id
unordered_map<CPUType_t, unordered_map<bool,unordered_map<VMType_t,vector<VMId_t>>>> CPUtoVM;

vector<struct MachineData> machineDataVect;
unordered_map<VMId_t, struct VMData> VMDataMap;
unordered_map<TaskId_t, VMId_t> taskToVM;
vector<vector<VMId_t>> machineToVM;
vector<bool> offline;
vector<bool> waking;

#define TASKS_PER_CPU 2
#define VMS_PER_MACHINE 2

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    offline = vector<bool>(Machine_GetTotal(), false);
    waking = vector<bool>(Machine_GetTotal(), false);
    
    //initialize CPUtoMachine and CPUtoVM
    for(unsigned i = 0; i < 4; i++) {

        unordered_map<bool,vector<MachineId_t>> GPUCheckMach;
        
        vector<MachineId_t> machsYes;
        vector<MachineId_t> machsNo;
        CPUtoMachine[(CPUType_t)i][true] = machsYes;
        CPUtoMachine[(CPUType_t)i][false] = machsNo;


        //CPU Type, GPU?, VM Type ==> VM Id
        unordered_map<bool, unordered_map<VMType_t,vector<VMId_t>>> GPUCheckVMs;
        CPUtoVM[(CPUType_t)i] = GPUCheckVMs;

        unordered_map<VMType_t, vector<VMId_t>> VMsYES;
        unordered_map<VMType_t, vector<VMId_t>> VMsNO;
        for(unsigned j = 0; j < 4; j++) {
            vector<VMId_t> vm_ids;
            VMsYES[(VMType_t)j] = vm_ids;
        }
        for(unsigned j = 0; j < 4; j++) {
            vector<VMId_t> vm_ids;
            VMsNO[(VMType_t)j] = vm_ids;
        }

        CPUtoVM[(CPUType_t)i][true] = VMsYES;
        CPUtoVM[(CPUType_t)i][false] = VMsNO;

    }

    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t machine = Machine_GetInfo(i);
        unsigned max_tasks = machine.num_cpus * TASKS_PER_CPU;
        unsigned mem_available = machine.memory_size;
        
        machineDataVect.push_back({mem_available, max_tasks, 0});

        vector<VMId_t> MTV;
        machineToVM.push_back(MTV);

        CPUtoMachine[machine.cpu][machine.gpus].push_back(i);
    }    
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    struct VMData vm_data = VMDataMap[vm_id];
    struct MachineData source_machine_data = machineDataVect[vm_data.source];
    MachineId_t original_source_id = vm_data.source;
    source_machine_data.mem_available += vm_data.mem_size;
    source_machine_data.task_count -= vm_data.task_count;
    machineDataVect[vm_data.source] = source_machine_data;

    vm_data.migrating = false;
    vm_data.source = vm_data.destination;
    VMDataMap[vm_id] = vm_data;
    migrating--;

    if(machineToVM[original_source_id].size() == 0 && Machine_GetInfo(original_source_id).s_state == S0 && !waking[original_source_id] && !offline[original_source_id]){
        offline[original_source_id] = true;
        Machine_SetState(original_source_id, S5);
    }
}

bool compareEfficiency(VMId_t a, VMId_t b){
    VMInfo_t vm_a = VM_GetInfo(a);
    MachineInfo_t machine_a = Machine_GetInfo(vm_a.machine_id);
    VMInfo_t vm_b = VM_GetInfo(b);
    MachineInfo_t machine_b = Machine_GetInfo(vm_b.machine_id);
    
    unsigned asum = (machine_b.c_states[0]+machine_b.p_states[0]+machine_b.s_states[0])*machine_a.performance[0];
    unsigned bsum = (machine_a.c_states[0]+machine_a.p_states[0]+machine_a.s_states[0])*machine_b.performance[0];
    return asum > bsum;

}

bool compareEfficiencyMachines(MachineId_t a, MachineId_t b){
    MachineInfo_t machine_a = Machine_GetInfo(a);
    MachineInfo_t machine_b = Machine_GetInfo(b);
    
    unsigned asum = (machine_b.c_states[0]+machine_b.p_states[0]+machine_b.s_states[0])*machine_a.performance[0];
    unsigned bsum = (machine_a.c_states[0]+machine_a.p_states[0]+machine_a.s_states[0])*machine_b.performance[0];
    return asum < bsum;

}

bool addTask(TaskId_t task_id, bool tryGpu) {

    TaskInfo_t task = GetTaskInfo(task_id);
    
    bool task_added = false;

    //add to a suitable VM that already exists
    vector<VMId_t> cpuandVMtype = CPUtoVM[task.required_cpu][tryGpu][task.required_vm];
    sort(cpuandVMtype.begin(), cpuandVMtype.end(), compareEfficiency);

    for(unsigned i = 0; i < cpuandVMtype.size(); i++){
        VMId_t vm_id = cpuandVMtype[i];
        VMInfo_t vm = VM_GetInfo(vm_id);
        struct VMData vm_data = VMDataMap[vm_id];
        MachineInfo_t machine = Machine_GetInfo(vm.machine_id);

        if(offline[vm.machine_id]){
            if(!waking[vm.machine_id] && machine.s_state != S0){
                waking[vm.machine_id] = true;
                Machine_SetState(vm.machine_id, S0);
            }
            continue;
        }

        struct MachineData vm_machine_data = machineDataVect[vm.machine_id];

        if(vm_data.migrating || vm_machine_data.task_count >= vm_machine_data.max_tasks || vm_machine_data.mem_available < task.required_memory){
            continue;
        }

        vm_data.task_count++;
        vm_data.mem_size += task.required_memory;
        vm_machine_data.task_count++;
        vm_machine_data.mem_available -= task.required_memory;
        VM_AddTask(vm_id, task_id, MID_PRIORITY);
        taskToVM[task_id] = vm_id;

        VMDataMap[vm_id] = vm_data;
        machineDataVect[vm.machine_id] = vm_machine_data;
        task_added = true;
        break;
    }

    //try and create a new VM
    if(!task_added){
        for(unsigned i = 0; i < CPUtoMachine[task.required_cpu][tryGpu].size(); i++){

            MachineId_t machine_id = CPUtoMachine[task.required_cpu][tryGpu][i];
            MachineInfo_t machine = Machine_GetInfo(machine_id);
            struct MachineData machine_data = machineDataVect[machine_id];

            if(offline[machine_id]){
                if(!waking[machine_id] && machine.s_state != S0){
                    waking[machine_id] = true;
                    Machine_SetState(machine_id, S0);
                }
                continue;
            }

            if(machine_data.task_count < machine_data.max_tasks && machine_data.mem_available >= task.required_memory
                && machineToVM[machine_id].size() < VMS_PER_MACHINE){

                VMId_t vm_id = VM_Create(task.required_vm, task.required_cpu);
                struct VMData vm_data = {1, task.required_memory + VM_MEMORY_OVERHEAD, machine_id, machine_id, false};
                struct MachineData vm_machine_data = machineDataVect[machine_id];
                vm_machine_data.task_count++;
                vm_machine_data.mem_available -= (task.required_memory + VM_MEMORY_OVERHEAD);
                VMDataMap[vm_id] = vm_data;
                VM_Attach(vm_id, machine_id);
                machineToVM[machine_id].push_back(vm_id);
                CPUtoVM[task.required_cpu][tryGpu][task.required_vm].push_back(vm_id);
                VM_AddTask(vm_id, task_id, MID_PRIORITY);
                taskToVM[task_id] = vm_id;
                task_added = true;

                VMDataMap[vm_id] = vm_data;
                machineDataVect[machine_id] = vm_machine_data;
                break;

            }
        }
    }

    return task_added;


}

void tryAddTask(TaskId_t task_id){
    TaskInfo_t task = GetTaskInfo(task_id);
    if(!addTask(task_id, task.gpu_capable) && !addTask(task_id, !task.gpu_capable)){
        unadded_tasks.push(task_id);
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    tryAddTask(task_id);
    
}


void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    // cout << unadded_tasks.size() << endl;
    retryUnaddedTasks();
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void destroyVM(VMId_t vm_id, VMInfo_t vm, struct VMData vm_data){

    //giving VM memory overhead back
    machineDataVect[vm.machine_id].mem_available += vm_data.mem_size;
    

    //removal from CPUtoVM
    //written by Github Copilot
    MachineInfo_t machine = Machine_GetInfo(vm.machine_id);
    machineToVM[vm.machine_id].erase(remove(machineToVM[vm.machine_id].begin(), machineToVM[vm.machine_id].end(), vm_id), machineToVM[vm.machine_id].end());
    CPUtoVM[vm.cpu][machine.gpus][vm.vm_type].erase(remove(CPUtoVM[vm.cpu][machine.gpus][vm.vm_type].begin(),
    CPUtoVM[vm.cpu][machine.gpus][vm.vm_type].end(), vm_id), CPUtoVM[vm.cpu][machine.gpus][vm.vm_type].end());

    VMDataMap.erase(vm_id);
    VM_Shutdown(vm_id);    
}

void removeTaskFromVM(TaskId_t task_id){
    VMId_t vm_id = taskToVM[task_id];
    VMInfo_t vm = VM_GetInfo(vm_id);
    struct VMData vm_data = VMDataMap[vm_id];
    struct MachineData vm_machine_data = machineDataVect[vm.machine_id];
    vm_data.task_count--;
    vm_data.mem_size -= GetTaskInfo(task_id).required_memory;
    vm_machine_data.task_count--;
    vm_machine_data.mem_available += GetTaskInfo(task_id).required_memory;
    VMDataMap[vm_id] = vm_data;
    machineDataVect[vm.machine_id] = vm_machine_data;
    taskToVM.erase(task_id);

    if(vm_data.task_count == 0 && !vm_data.migrating){
        destroyVM(vm_id, vm, vm_data);
    }
    

}



void migrate(){

    for(unsigned CPUType = 0; CPUType < 4; CPUType++){
        for(unsigned gpu = 0; gpu < 2; gpu++){
            vector<MachineId_t> machinesCopy = CPUtoMachine[(CPUType_t) CPUType][(bool) gpu];
            if(machinesCopy.size() == 0){
                continue;
            }

            sort(machinesCopy.begin(), machinesCopy.end(), compareEfficiencyMachines);

            unsigned index = 0;
            for(unsigned i = machinesCopy.size() - 1; i > 0; i--){
                if(machineDataVect[machinesCopy[i]].task_count > 0){
                    index = i;
                    break;
                }
            }


            bool found = false;
            while(index > 0 && !found){
                MachineId_t source_machine_id = machinesCopy[index];
                MachineInfo_t source_machine = Machine_GetInfo(source_machine_id);
                struct MachineData source_machine_data = machineDataVect[source_machine_id];
            
                vector<VMId_t> source_vms = machineToVM[source_machine_id];
            
                for(unsigned dest_index = index - 1; dest_index > 0; dest_index--){
                    
                    MachineId_t destination_id = machinesCopy[dest_index];
                    MachineInfo_t destination_machine = Machine_GetInfo(destination_id);
                    struct MachineData destination_machine_data = machineDataVect[destination_id];
                
                    if(destination_machine_data.task_count == destination_machine_data.max_tasks || machineToVM[destination_id].size() >= VMS_PER_MACHINE || offline[destination_id]){
                        continue;
                    }
                
                    for(unsigned source_vm_index = 0; source_vm_index < source_vms.size(); source_vm_index++){
                        
                        VMId_t source_vm_id = source_vms[source_vm_index];
                        struct VMData source_vm_data = VMDataMap[source_vm_id];
                        VMInfo_t source_vm = VM_GetInfo(source_vm_id);
                        if(source_vm_data.migrating || source_machine.cpu != destination_machine.cpu || source_machine.gpus != destination_machine.gpus ||
                            source_vm_data.task_count == 0 || source_vm.machine_id != source_machine_id || destination_machine_data.mem_available < source_vm_data.mem_size
                            || source_vm_data.task_count + destination_machine_data.task_count > destination_machine_data.max_tasks){
                            continue;
                        }
                    
                        found = true;
                        migrating++;
                    
                        //vector eraser written with help from Github Copilot
                        machineToVM[source_machine_id].erase(remove(machineToVM[source_machine_id].begin(), machineToVM[source_machine_id].end(), source_vm_id), machineToVM[source_machine_id].end());
                        source_vm_data.migrating = true;
                        source_vm_data.destination = destination_id;
                        source_vm_data.source = source_machine_id;
                    
                        machineToVM[destination_id].push_back(source_vm_id);
                        destination_machine_data.task_count += source_vm_data.task_count;
                        assert(destination_machine_data.mem_available >= source_vm_data.mem_size);
                        destination_machine_data.mem_available -= source_vm_data.mem_size;
                    
                        VMDataMap[source_vm_id] = source_vm_data;
                        machineDataVect[source_machine_id] = source_machine_data;
                        machineDataVect[destination_id] = destination_machine_data;
                        VM_Migrate(source_vm_id, destination_id);
                    
                    }
                }
            
            
                index--;       
            }
        }
    }


    

}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

    removeTaskFromVM(task_id);
    if(!migrating){
        migrate();
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

    retryUnaddedTasks();

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
    MachineInfo_t machine = Machine_GetInfo(machine_id);
    if(machine.s_state == S0){
        waking[machine_id] = false;
        offline[machine_id] = false;
    }
}

void retryUnaddedTasks(){
    unsigned iterations = unadded_tasks.size();
    for(unsigned i = 0; i < iterations; i++)
    {
        if(unadded_tasks.empty())
            break;
        TaskId_t unadded_id = unadded_tasks.front();
        unadded_tasks.pop();
        tryAddTask(unadded_id);
    }
}
