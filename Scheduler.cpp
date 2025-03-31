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
#include <queue>

vector<vector<VMId_t>> machineToVM;
unordered_map<TaskId_t, VMId_t> taskToVM;
vector<vector<TaskId_t>> unadded_tasks;

vector<vector<vector<MachineId_t>>> activeMachines;
vector<vector<vector<MachineId_t>>> intrMachines;
vector<vector<vector<MachineId_t>>> offMachines;
vector<vector<unsigned>> intrMachine_constants;

vector<vector<vector<MachineId_t>>> machinesBecomingActive;
vector<vector<vector<MachineId_t>>> machinesBecomingIntr;
vector<vector<vector<MachineId_t>>> machinesBecomingOff;


unsigned VMSize(VMId_t vm_id);
unsigned estimatedMemoryAvailable(MachineId_t machine_id);
unsigned estimatedActiveTasks(MachineId_t machine_id);
unsigned estimatedAvailableTasks(MachineId_t machine_id);
bool overloadCheck(CPUType_t cpu, bool gpu);

#define TASKS_PER_CPU 2

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


    for (int i = 0; i < 4; i++) {
        vector<TaskId_t> taskList;
        unadded_tasks.push_back(taskList);
    }
    for (int i = 0; i < 4; i++){
        vector<vector<MachineId_t>> machineActiveList;
        activeMachines.push_back(machineActiveList);
        vector<vector<MachineId_t>> machineIntrList;
        intrMachines.push_back(machineIntrList);
        vector<vector<MachineId_t>> machineShutdownList;
        offMachines.push_back(machineShutdownList);
        vector<unsigned> intrMachine_constantList;
        intrMachine_constants.push_back(intrMachine_constantList);
        for (int j = 0; j < 2; j++){
            vector<MachineId_t> subActiveList;
            activeMachines[i].push_back(subActiveList);
            vector<MachineId_t> subIntrList;
            intrMachines[i].push_back(subIntrList);
            vector<MachineId_t> subShutdownList;
            offMachines[i].push_back(subShutdownList);
            intrMachine_constants[i].push_back(0);
        }
    }
    

    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        machines.push_back(MachineId_t(i));
        vector<VMId_t> MTV;
        machineToVM.push_back(MTV);
        intrMachine_constants[Machine_GetCPUType(MachineId_t(i))][Machine_GetInfo(MachineId_t(i)).gpus]++;
        //put all machines in shutdown for now
        offMachines[Machine_GetCPUType(MachineId_t(i))][Machine_GetInfo(MachineId_t(i)).gpus].push_back(MachineId_t(i));
    } 
    for (unsigned i = 0; i < intrMachine_constants.size();i++){
        for (unsigned  j = 0; j < intrMachine_constants[i].size();j++){
            intrMachine_constants[i][j] = intrMachine_constants[i][j]/3;
        }   
    }

    //setting 0.1 of the machines to be in the intermediate section
    for(unsigned i = 0; i < intrMachine_constants.size(); i++){
        
        vector<vector<MachineId_t>> machineIds;
        vector<vector<MachineId_t>> machineIds2;
        machinesBecomingIntr.push_back(machineIds);
        machinesBecomingOff.push_back(machineIds2);
        
        for(unsigned j = 0; j < intrMachine_constants[i].size(); j++){
            vector<MachineId_t> subMachineIds;
            vector<MachineId_t> subMachineIds2;
            machinesBecomingIntr[i].push_back(subMachineIds);
            machinesBecomingOff[i].push_back(subMachineIds2);
            for (unsigned k = 0; k < intrMachine_constants[i][j]; k++){
                Machine_SetState(offMachines[i][j].back(),S1);
                machinesBecomingIntr[i][j].push_back(offMachines[i][j].back());
                offMachines[i][j].pop_back();
            }
            
        }
    }

    //actually shutdown all machines in off
    for (unsigned i = 0; i < offMachines.size(); i++){
        vector<vector<MachineId_t>> machineIds;
        machinesBecomingActive.push_back(machineIds);
        for(unsigned j = 0; j < offMachines[i].size(); j++){
            vector<MachineId_t> subMachineIds;
            machinesBecomingActive[i].push_back(subMachineIds);
            for(unsigned k = 0; k < offMachines[i][j].size(); k++){
                Machine_SetState(offMachines[i][j][k],S3);
                machinesBecomingOff[i][j].push_back(offMachines[i][j][k]);
            }
            offMachines[i][j].clear();
            
        }
        //all machines are removed from list until state change is finished
    }

    // // SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
}


void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Unused in E-Eco  
}

bool addTask(TaskId_t task_id, bool tryGpu) {
    TaskInfo_t task = GetTaskInfo(task_id);
    bool task_added = false;
    for(unsigned i = 0; i < activeMachines[task.required_cpu][tryGpu].size(); i++)
    {
        MachineInfo_t currentMachine = Machine_GetInfo(activeMachines[task.required_cpu][tryGpu][i]);

        if(estimatedMemoryAvailable(currentMachine.machine_id) > (task.required_memory + 8) && (currentMachine.num_cpus * TASKS_PER_CPU - estimatedActiveTasks(currentMachine.machine_id) > 0)){
            
            for(unsigned j = 0; j < machineToVM[currentMachine.machine_id].size(); j++){
                VMInfo_t currentVM = VM_GetInfo(machineToVM[currentMachine.machine_id][j]);
                if(currentVM.vm_type == task.required_vm ){
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
                machineToVM[currentMachine.machine_id].push_back(newVM);
                taskToVM[task.task_id] = newVM;
                task_added = true;
            }
            break;
            

        }
        else if (estimatedMemoryAvailable(currentMachine.machine_id) > task.required_memory && currentMachine.num_cpus * TASKS_PER_CPU - estimatedActiveTasks(currentMachine.machine_id) > 0){
            for(unsigned j = 0; j < machineToVM[i].size(); j++){
                VMInfo_t currentVM = VM_GetInfo(machineToVM[currentMachine.machine_id][j]);
                if(currentVM.vm_type == task.required_vm ){
                    VM_AddTask(currentVM.vm_id, task.task_id, MID_PRIORITY);
                    taskToVM[task.task_id] = currentVM.vm_id;
                    task_added = true;
                    break;
                }
            }
            break;
            

        }


    }
    return task_added;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {

    TaskInfo_t task = GetTaskInfo(task_id);
    bool task_added = true;
    bool no_prefered_machine = false;
    
    if ( !addTask(task_id, task.gpu_capable)){
        task_added = addTask(task_id, !task.gpu_capable);
        no_prefered_machine = true;
    }
    
    bool intrpool_changed = false;
    if (task_added){
        //calculate if "over loaded"/ nearing that point
        //arbitrarily when task to CPU ratio of 1.5
        
        if (overloadCheck(task.required_cpu,no_prefered_machine^task.gpu_capable)){
            //active machines are overloaded! move some from intrpool to active!

            if (intrMachines[task.required_cpu][task.gpu_capable].size()>0){
                Machine_SetState(intrMachines[task.required_cpu][task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][task.gpu_capable].push_back(intrMachines[task.required_cpu][task.gpu_capable].back());
                intrMachines[task.required_cpu][task.gpu_capable].pop_back();
                intrpool_changed = true;
            }else if (offMachines[task.required_cpu][task.gpu_capable].size()>0){
                Machine_SetState(offMachines[task.required_cpu][task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][task.gpu_capable].push_back(offMachines[task.required_cpu][task.gpu_capable].back());
                offMachines[task.required_cpu][task.gpu_capable].pop_back();
            }
        }

    }
    

  
    if(!task_added){
        //Task can't be added, add new machine
        unadded_tasks[GetTaskInfo(task_id).required_cpu].push_back(task_id);
        //try to get machine of perferred GPU first, then other machines
        if (machinesBecomingActive[task.required_cpu][task.gpu_capable].size() < 1){
            if (intrMachines[task.required_cpu][task.gpu_capable].size() > 0){
                Machine_SetState(intrMachines[task.required_cpu][task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][task.gpu_capable].push_back(intrMachines[task.required_cpu][task.gpu_capable].back());
                intrMachines[task.required_cpu][task.gpu_capable].pop_back();
                intrpool_changed = true;
            }else if (offMachines[task.required_cpu][task.gpu_capable].size()>0){
                Machine_SetState(offMachines[task.required_cpu][task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][task.gpu_capable].push_back(offMachines[task.required_cpu][task.gpu_capable].back());
                offMachines[task.required_cpu][task.gpu_capable].pop_back();
            }else if (intrMachines[task.required_cpu][!task.gpu_capable].size() > 0){
                Machine_SetState(intrMachines[task.required_cpu][!task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][!task.gpu_capable].push_back(intrMachines[task.required_cpu][!task.gpu_capable].back());
                intrMachines[task.required_cpu][!task.gpu_capable].pop_back();
                intrpool_changed = true;
            }else if (offMachines[task.required_cpu][!task.gpu_capable].size()>0){
                Machine_SetState(offMachines[task.required_cpu][!task.gpu_capable].back(),S0);
                machinesBecomingActive[task.required_cpu][!task.gpu_capable].push_back(offMachines[task.required_cpu][!task.gpu_capable].back());
                offMachines[task.required_cpu][!task.gpu_capable].pop_back();
            }
         }
    }
    
    //check balance of intermediate
    if (intrpool_changed ){
        for (unsigned i = 0; i < intrMachines.size(); i++) {
            for (unsigned j = 0; j < intrMachines[i].size(); j++) {
                if (intrMachines[i][j].size() + machinesBecomingIntr[i][j].size() <intrMachine_constants[i][j]){
                    //below quota, need to have more intr machines
                    while(intrMachines[i][j].size() + machinesBecomingIntr[i][j].size() <intrMachine_constants[i][j] && offMachines[i][j].size()>0){
                        if (offMachines[i][j].size()>0){
                            Machine_SetState(offMachines[i][j].back(),S1);
                            machinesBecomingIntr[i][j].push_back(offMachines[i][j].back());
                            offMachines[i][j].pop_back();
                        }
                    }
                }
            }
        }
    }
    


}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
 
    for(unsigned i = 0; i < unadded_tasks.size(); i++){
        
        unsigned size = unadded_tasks[i].size();
        for (unsigned j = 0 ; j < size; j++){

            TaskId_t read_id = unadded_tasks[i].front();
            unadded_tasks[i].erase(unadded_tasks[i].begin());
            
            NewTask(now, read_id);
        }
    }

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

    //if VM empty, destroy
    VMInfo_t completedTaskVM = VM_GetInfo(taskToVM[task_id]);
    MachineInfo_t completedMachine = Machine_GetInfo(completedTaskVM.machine_id);

    if(completedTaskVM.active_tasks.size() == 0){

        for(unsigned i = 0; i < machineToVM[completedTaskVM.machine_id].size(); i++){
            if(machineToVM[completedTaskVM.machine_id][i] == completedTaskVM.vm_id){
                machineToVM[completedTaskVM.machine_id].erase(machineToVM[completedTaskVM.machine_id].begin() + i);
                VM_Shutdown(completedTaskVM.vm_id);
                break;
            }
        }
    }

    if  (completedMachine.active_tasks == 0){
        //remove from active pool to see if doing so will overload the active pool
        for (unsigned i = 0; i < activeMachines[completedMachine.cpu][completedMachine.gpus].size(); i++){
            if (activeMachines[completedMachine.cpu][completedMachine.gpus][i] == completedMachine.machine_id){
                activeMachines[completedMachine.cpu][completedMachine.gpus].erase(activeMachines[completedMachine.cpu][completedMachine.gpus].begin() + i);
                break;
            }
        }
        //if not overloaded without machine, send to intermediate
        if (!overloadCheck(completedMachine.cpu,completedMachine.gpus)){
            machinesBecomingIntr[completedMachine.cpu][completedMachine.gpus].push_back(completedMachine.machine_id);
            Machine_SetState(completedMachine.machine_id,S1);
        }else{ // else add it back to active list
            activeMachines[completedMachine.cpu][completedMachine.gpus].push_back(completedMachine.machine_id);
        }
        
    }
 

    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

bool overloadCheck(CPUType_t cpu, bool gpu){
    int totalTasks = 0;
    int totalCPUs = 0;
    for (unsigned i = 0; i < activeMachines[cpu][gpu].size(); i++){
        totalTasks += Machine_GetInfo(activeMachines[cpu][gpu][i]).active_tasks;
        totalCPUs += Machine_GetInfo(activeMachines[cpu][gpu][i]).num_cpus;
    }
    //count machines that are becoming active
    for (unsigned i = 0; i < machinesBecomingActive[cpu][gpu].size(); i++){
        totalCPUs += Machine_GetInfo(machinesBecomingActive[cpu][gpu][i]).num_cpus;
    }
    //done strangely to avoid floatingpoints

    return 2*totalTasks >= 3*totalCPUs;

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
    
    MachineInfo_t machine = Machine_GetInfo(machine_id);
 
    //determining which pool to place machine after state change
    if (machine.s_state == S1){
        intrMachines[machine.cpu][machine.gpus].push_back(machine_id);
        for(unsigned i = 0; i < machinesBecomingIntr[machine.cpu][machine.gpus].size(); i++){
            if (machinesBecomingIntr[machine.cpu][machine.gpus][i] == machine_id){
                machinesBecomingIntr[machine.cpu][machine.gpus].erase(machinesBecomingIntr[machine.cpu][machine.gpus].begin()+i);
                break;
            }
        }
    }else if(machine.s_state == S3){
        offMachines[machine.cpu][machine.gpus].push_back(machine_id);
        for(unsigned i = 0; i < machinesBecomingOff[machine.cpu][machine.gpus].size(); i++){
            if (machinesBecomingOff[machine.cpu][machine.gpus][i] == machine_id){
                machinesBecomingOff[machine.cpu][machine.gpus].erase(machinesBecomingOff[machine.cpu][machine.gpus].begin()+i);
            }
        }
    }else{
        if(unadded_tasks[machine.cpu].size() != 0) {
            TaskId_t task = unadded_tasks[machine.cpu].front();
            unadded_tasks[machine.cpu].erase(unadded_tasks[machine.cpu].begin());
            VMId_t newVM = VM_Create(GetTaskInfo(task).required_vm, GetTaskInfo(task).required_cpu);
            VM_Attach(newVM, machine.machine_id);
            VM_AddTask(newVM, task, MID_PRIORITY);
            machineToVM[machine.machine_id].push_back(newVM);
            taskToVM[task] = newVM;
        }
        for(unsigned i = 0; i < machinesBecomingActive[machine.cpu][machine.gpus].size(); i++){
            if (machinesBecomingActive[machine.cpu][machine.gpus][i] == machine_id){
                machinesBecomingActive[machine.cpu][machine.gpus].erase(machinesBecomingActive[machine.cpu][machine.gpus].begin()+i);
            }
        }
        activeMachines[machine.cpu][machine.gpus].push_back(machine_id);

    }
}

unsigned estimatedMemoryAvailable(MachineId_t machine_id){
    MachineInfo_t machine = Machine_GetInfo(machine_id);
    return machine.memory_size - machine.memory_used;
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
    return machine.active_tasks;
}

unsigned estimatedAvailableTasks(MachineId_t machine_id){
    MachineInfo_t machine = Machine_GetInfo(machine_id);

    unsigned est_active = estimatedActiveTasks(machine_id);
    return machine.num_cpus * TASKS_PER_CPU - est_active;
}