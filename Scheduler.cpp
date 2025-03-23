//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

// TODO: make all the machines run
// Have some greedy strategy
// Setup VMs for all machines (try immediately and as tasks come)
// Multiple VMs on a single machine

#include "Scheduler.hpp"
#include <vector>
#include <bits/stdc++.h>
#include <unordered_map>

unsigned estimatedMemoryAvailable(MachineId_t machine_id);
unsigned VMSize(VMId_t vm_id);
unsigned estimatedActiveTasks(MachineId_t machine_id);

static bool migrating = false;
static vector<vector<VMId_t>> MachinesToVMs;
static unordered_map<VMId_t, MachineId_t> migrationMap;



void Scheduler::Init() {
    cout << "Hello :D" << endl;
    
    cout << "Machine Count: " << Machine_GetTotal() << endl;
    // for(unsigned i = 0; i <  Machine_GetTotal(); i++){
    //     cout << Machine_GetInfo(MachineId_t(i)).p_state << endl;
    // }
    
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
    // for(unsigned i = 0; i < active_machines * 4; i++)
    //     vms.push_back(VM_Create(LINUX, X86));
    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        machines.push_back(MachineId_t(i));
        vector<VMId_t> MTV;
        MachinesToVMs.push_back(MTV);
        
    }    
    // for(unsigned i = 0; i < active_machines * 4; i++) {
    //     // cout << machines[i] << " memory used " << Machine_GetInfo(machines[i]).memory_used << endl;
    //     VM_Attach(vms[i], machines[i/4]);
    //     // cout << machines[i] << " memory used " << Machine_GetInfo(machines[i]).memory_used << endl;
    // }

    // bool dynamic = false;
    // if(dynamic)
    //     for(unsigned i = 0; i<4 ; i++)
    //         for(unsigned j = 0; j < 8; j++)
    //             Machine_SetCorePerformance(MachineId_t(0), j, P3);
    // Turn off the ARM machines
    // for(unsigned i = 24; i < Machine_GetTotal(); i++)
    //     Machine_SetState(MachineId_t(i), S5);

    // SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);

    
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // cout << "In NewTask, task_id: " << task_id << endl;
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
        //  VM vm(type of the VM)
        //  vm.Attach(machine_id);
        //  vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    // Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    // if(migrating) {
    //     VM_AddTask(vms[0], task_id, priority);
    // }
    // else {
    //     unsigned min = 99999;
    //     unsigned mindex = 0;
    //     for(unsigned i = 0; i < vms.size() ; i++){
    //         unsigned active = VM_GetInfo((VMId_t) i).active_tasks.size();
    //         if(active < min){
    //             min = active;
    //             mindex = i;
    //         }
    //     }
    //     // cout << "mindex : " << mindex << endl; 
    //     VM_AddTask(vms[mindex], task_id, priority);
    // }// Skeleton code, you need to change it according to your algorithm
    for(unsigned i = 0; i < Machine_GetTotal(); i++){
        MachineInfo_t machine = Machine_GetInfo(machines[i]);
        TaskInfo_t task = GetTaskInfo(task_id);

        if(task.required_cpu != machine.cpu){
            continue;
        }
        
        int tasks_available = machine.num_cpus * 2 - estimatedActiveTasks(machine.machine_id);
        unsigned memory_available = estimatedMemoryAvailable(machine.machine_id);
        
        // if(machine.active_tasks > 0){
        //     cout << "Machine " << machine.machine_id << "has " << machine.active_tasks << " active tasks" << endl;
        // }
        //cout << "Tasks active " <<  tasks_available << endl;
        //3 cases
        // can fit, VM exists
        bool vm_found = false;
        if(tasks_available > 0 && memory_available >= (task.required_memory))
        {
            for(unsigned j = 0; j < MachinesToVMs[i].size(); j++){
                if(VM_GetInfo(MachinesToVMs[i][j]).vm_type == task.required_vm && !migrationMap.count(MachinesToVMs[i][j]))
                {
                    VM_AddTask(MachinesToVMs[i][j], task_id, MID_PRIORITY);
                    // cout << "Task " << task_id << " added case 1 to machine " << machines[i] << endl;
                    vm_found = true;
                    break;
                }
            }
            if(vm_found){
                break;
            }
        }
        // can fit, VM doesn't exist (need +8 for VM)
        if(!vm_found)
        {
            if(tasks_available > 0 && memory_available >= (task.required_memory + 8))
            {
                VMId_t newVm = VM_Create(task.required_vm, machine.cpu);
                VM_Attach(newVm, machines[i]);
                // cout << "VM: " << newVm << " added to machine " << machines[i] << endl;
                MachinesToVMs[i].push_back(newVm);
                VM_AddTask(newVm, task_id, MID_PRIORITY);
                // cout << "Task added case 2!" << endl;
                vm_found = true;
                break;
            }
        }
        
        // can't fit
    }
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
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy

    vector<MachineId_t> machinesCopy = machines;
    unordered_map<VMId_t, vector<MachineId_t>> destinationMap;

    // cout << "A" << endl;

    // sorts in ascending order by utilization measured as active tasks
    sort(machinesCopy.begin(), machinesCopy.end(), [](MachineId_t a, MachineId_t b) {return Machine_GetInfo(a).active_tasks * Machine_GetInfo(b).num_cpus <  Machine_GetInfo(b).active_tasks * Machine_GetInfo(a).num_cpus; });
    

    //find first machine > 0 active tasks
    int CurrentMachine = -1;
    for(unsigned i = 0; i < machinesCopy.size(); i++){
        if(Machine_GetInfo(machines[i]).active_tasks > 0){
            CurrentMachine = i;
            break;
        }
    } 

    if(CurrentMachine == -1)
    {
        // cout << "no machine found with > 0 active tasks" << endl;
        return;
    }


    // cout << "B" << endl;
    while((unsigned) CurrentMachine < machinesCopy.size()){
        MachineId_t CurrentMachineId = machinesCopy[CurrentMachine];
        
        for(unsigned i = 0; i < MachinesToVMs[CurrentMachineId].size(); i++){
            
            VMInfo_t currentVM = VM_GetInfo(MachinesToVMs[CurrentMachineId][i]);
            unsigned currentVMSize = VMSize(currentVM.vm_id);

            if(migrationMap.count(currentVM.vm_id)){
                continue;
            }

            for(unsigned j = CurrentMachine + 1; j < machinesCopy.size(); j++){
                MachineInfo_t destination = Machine_GetInfo(machinesCopy[j]);

                if(destination.cpu != currentVM.cpu){
                    continue;
                }
                // cout << "C" << endl;
                unsigned availableMemory = estimatedMemoryAvailable(machinesCopy[j]);

                if(Machine_GetInfo(machinesCopy[j]).num_cpus * 2 - estimatedActiveTasks(machinesCopy[j]) - currentVM.active_tasks.size() >= 0 && availableMemory >= currentVMSize){
                    //add to destination map
                    vector<MachineId_t> srcDest = {(MachineId_t) CurrentMachine, machinesCopy[j]};
                    destinationMap[currentVM.vm_id] = srcDest;
                    //add to other machinestovms
                    MachinesToVMs[machinesCopy[j]].push_back(currentVM.vm_id);
                    break;
                }

            }

        }

        CurrentMachine++;


    }

    //migrate all in destination map
    //update migration map
    for(auto it = destinationMap.begin(); it != destinationMap.end(); it++){
        migrationMap[it->first] = it->second.at(0);
        VM_Migrate(it->first, it->second.at(1));
        cout << "migrating " << it->first << " to " << it->second.at(1) << endl;
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
    // cout << "HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time) << endl;
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

    //remove from source machinestovms
    MachineId_t source = migrationMap[vm_id];
    for(unsigned k = 0; k < MachinesToVMs[source].size(); k++){
        if(MachinesToVMs[source][k] == vm_id){
            MachinesToVMs[source].erase(MachinesToVMs[source].begin() + k);
        }
    }

    migrating = false;
    migrationMap.erase(vm_id);
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    // static unsigned counts = 0;
    // counts++;
    // if(counts == 10) {
    //     migrating = true;
    //     VM_Migrate(1, 9);
    // }
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
    for(unsigned i = 0; i < MachinesToVMs[machine_id].size(); i++){
        mem_used += VMSize(MachinesToVMs[machine_id][i]);
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
    for(unsigned i = 0; i < MachinesToVMs[machine_id].size(); i++){
        tasks += VM_GetInfo(MachinesToVMs[machine_id][i]).active_tasks.size();
    }
    return tasks;
}
