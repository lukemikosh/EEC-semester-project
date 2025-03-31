To run this scheduler, compile the Scheduler with make scheduler and run make simulator to create your simulator executable. Run ./simulator Input.md to see results.

This is an approximation of the E-Eco algorithm. It maintains 3 pools of machines at different S-states. Tasks are assigned to running machines. If there are none avaliable, machines from an intermediate pool will be assigned to the active pool, and machines from the off pool will be moved to the intermendiate pool to maintain its size. This algorithm considers GPUs and attempts to assign GPU capable tasks to machines with GPUs.
