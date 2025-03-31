To run this scheduler, compile the Scheduler with `make scheduler` and run `make simulator` to create your simulator executable. Run `./simulator Input.md` to see results.

This is a greedy scheduler algorithm that assigns tasks to the first avaliable machine. The greedy algorithm will mirate virtual machines to highly utilized machines to attempt to turn off machines. This algorithm also considers GPUs, attempting to assign GPU capable tasks to machines with GPUs.

Some portions of this code were assisted by GitHub Copilot.
