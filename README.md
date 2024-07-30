# Map Reduce System

### A program that uses thread parallelism to mimic a distributed MapReduce system with a coordinator and worker processes. 
<ul>
    <li>Utilized mutex locks to prevent data races across worker and coordinator processes. </li>
    <li>Added fault-tolerance & improved task completion speed by redistributing tasks from slow, assumed crashed, workers.</li>
    <li>Improved performance by having backup executions of remaining in-progress tasks.</li>
    <li>Used RPC protocol to simulate communication between worker and coordinator machines.</li>
    <li>Stored intermediate output from workers' map tasks in JSON files to mimic local disk storage</li>
</ul>

## The relevant files are in the mr/ directory

Inspired by the <a href="https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf">MapReduce paper</a>