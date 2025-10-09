# Chord

We create a chord network. We use random integers on a very large domain for the ids because for this simulation, this is essentially the same as a hash function.

Importantly, our implementation is asynchronous, while the pseudo code in the paper is synchronous. This is done by creating a requests table where nodes store the requests that they have sent out and are waiting for a reply on. When they receive a reply, they pop the result from the table and continue with what they were doing. In find successor, nodes only pass the message on without waiting for a reply, because the node that finds the result will send it back to the original requestor. This implementation enables parallel execution and avoids the deadlocks that could easily happen in the original Chord pseudo code, but it is much easier to track what each person is searching for making this scheme less private.

I ran this command `gleam run 4098 2 > hops.txt` and analyzed the results with the following python code.

```
import numpy as np
r = np.loadtxt('hops.txt')
r.mean()
```

The resulting average number of hops was `5.8715`, which is consistent with the logorithmic time expectation. I ran one with 10,000 nodes and got 6.5146 average hops.

We initiate the network. An example run with the following main function was run where we had more print statements.

```
let nodes = make_ring(16, 512)
echo list.unzip(nodes).0
let assert Ok(n) = list.first(nodes)

io.println("Searching for file key 1000")
process.send(n.1, SearchFileKey(1000, n.1))
process.sleep(100)

let nodes = [add_node(n), ..nodes]
let nodes = [add_node(n), ..nodes]
let nodes = [add_node(n), ..nodes]
let nodes = [add_node(n), ..nodes]
process.sleep(100)

fix_all(nodes)
process.sleep(100)
// echo n.0
io.println("Searching for file key 300000000")
process.send(n.1, SearchFileKey(300_000_000, n.1))
process.send(n.1, SearchFileKey(1000, n.1))
process.send(n.1, SearchFileKey(1_000_000_000, n.1))
process.sleep(1000)
```

The results are shown in `output.txt`. This shows the ring being traversed in logorithmic time, new nodes being added to the network with finger tables, successors and predecessors being updated.
