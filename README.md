To run this scheduler, compile the Scheduler with make scheduler and run make simulator to create your simulator executable. Run ./simulator Input.md to see results.

This is an approximation of a P-Mapper algorithm that assigns tasks to machines that use the least energy. When a task finishes, it will migrate virtual machines to more energy efficent machines to save power. Unused machines will be powered off.This algorithm also considers GPUs, attempting to assign GPU capable tasks to machines with GPUs.

Some portions of this code were assisted by GitHub Copilot.
