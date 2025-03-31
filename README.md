To run this scheduler, compile the Scheduler with make scheduler and run make simulator to create your simulator executable. Run ./simulator Input.md to see results.

This is an approximation of the snooze algorithm that assigns tasks to the first avaliable machine. When a task finishes, the algorithm will migrate vms to higher utilized machines only if it can migrate all vms on a machine. Unused machines will be powered off. This algorithm also considers GPUs, attempting to assign GPU capable tasks to machines with GPUs.

Some portions of this code were assisted by GitHub Copilot.
