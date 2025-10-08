import gleam/bool
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor

// import gleam/crypto

fn find_closest_preceding_finger(
  n: Int,
  id: Int,
  finger_table: List(#(Int, process.Subject(NodeMsg))),
) {
  case finger_table {
    [] -> Error(Nil)
    [head, ..tail] -> {
      let #(finger_id, _finger_node) = head
      case list.length(tail) {
        0 -> Ok(head)
        _ -> {
          // if finger_id is between n and id on the circle in a clockwise way
          case
            {
              { { n < finger_id } && { finger_id < id } }
              || { { n > id } && { { n < finger_id } || { finger_id < id } } }
            }
          {
            True -> Ok(head)
            False -> find_closest_preceding_finger(n, id, tail)
          }
        }
      }
    }
  }
}

pub type NodeMsg {
  // Find the successor of id
  FindSuccessor(
    id: Int,
    reply_to: process.Subject(NodeMsg),
    update_finger: Bool,
  )
  SuccessorResult(result: #(Int, process.Subject(NodeMsg)), update_finger: Bool)
  SetFingers(finger_table: List(#(Int, process.Subject(NodeMsg))))
  Join(
    id: Int,
    node: process.Subject(NodeMsg),
    reply_to: process.Subject(NodeMsg),
  )
  Stablize(
    pred_of_succ: option.Option(#(Int, process.Subject(NodeMsg))),
    reply_to: #(Int, process.Subject(NodeMsg)),
  )
  SetKeys(keys: List(Int))
  AddKey(key: Int)
  Notify(node: #(Int, process.Subject(NodeMsg)))
  SetPredecessor(#(Int, process.Subject(NodeMsg)))
  RequestID(
    reply_to: #(Int, process.Subject(NodeMsg)),
    node: process.Subject(NodeMsg),
  )
  // Ask the predecessor to send its id and then set predecessor
  RequestPred(reply_to: #(Int, process.Subject(NodeMsg)))
  // Ask the successor to send its predecessor and then stablize
  // PredecessorResponse(Maybe(process.Subject(NodeMsg)))
  ShutDown
}

pub type NodeState {
  NodeState(
    id: Int,
    file_keys: List(Int),
    predecessor: option.Option(#(Int, process.Subject(NodeMsg))),
    finger_table: List(#(Int, process.Subject(NodeMsg))),
    requests_table: List(Int),
    //    boss: process.Subject(BossMsg),
  )
}

fn start_node(id: Int) -> process.Subject(NodeMsg) {
  let init = NodeState(id, [], option.None, [], [])

  let builder =
    actor.new(init)
    |> actor.on_message(fn(state, msg) {
      case msg {
        ShutDown -> actor.stop()

        SetFingers(finger_table) -> {
          // sort fingers
          let assert Ok(m) = int.power(2, 30.0)
          let m = float.round(m)
          let finger_table =
            list.sort(finger_table, fn(a, b) {
              int.compare(
                { a.0 + m - state.id } % m,
                { b.0 + m - state.id } % m,
              )
            })
          // echo [state.id, ..list.unzip(finger_table).0]
          actor.continue(NodeState(..state, finger_table: finger_table))
        }

        SetPredecessor(node) -> {
          // io.println(
          //   "Node "
          //   <> int.to_string(state.id)
          //   <> "'s predecessor is "
          //   <> int.to_string(node.0),
          // )
          actor.continue(NodeState(..state, predecessor: option.Some(node)))
        }

        SetKeys(keys) -> {
          // echo [state.id, ..keys]
          actor.continue(NodeState(..state, file_keys: keys))
        }

        SuccessorResult(result, update_finger) -> {
          io.println("Got result! The Id is " <> int.to_string(result.0))
          case update_finger {
            False -> actor.continue(state)
            True -> {
              io.println(
                "New node "
                <> int.to_string(id)
                <> " joined! Its successor is "
                <> int.to_string(result.0),
              )
              // Set the successor for the newly joined node
              actor.continue(NodeState(..state, finger_table: [result]))
            }
          }
        }

        FindSuccessor(id, reply_to, update_finger) -> {
          let assert Ok(successor) = list.first(state.finger_table)
          let found =
            { { state.id < id } && { id < successor.0 } }
            || {
              { state.id > successor.0 }
              && { { state.id < id } || { id < successor.0 } }
            }
          case found {
            True -> {
              io.println(
                "node "
                <> int.to_string(state.id)
                <> " found successor of "
                <> int.to_string(id),
              )
              // send successor to reply_to
              process.send(reply_to, SuccessorResult(successor, update_finger))
              process.send(reply_to, RequestID(successor, reply_to))
              //
              actor.continue(state)
            }
            False -> {
              let assert Ok(closest_preceding_finger) =
                find_closest_preceding_finger(
                  state.id,
                  id,
                  list.reverse(state.finger_table),
                )
              io.println(
                "node "
                <> int.to_string(state.id)
                <> " forwarding to finger "
                <> int.to_string(closest_preceding_finger.0),
              )
              process.sleep(100)
              // forward the message to closest_preceding_finger
              process.send(
                closest_preceding_finger.1,
                FindSuccessor(id, reply_to, update_finger),
              )
              actor.continue(state)
            }
          }
        }

        Join(id, node, reply_to) -> {
          process.send(node, FindSuccessor(id, reply_to, True))
          actor.continue(state)
        }

        Stablize(pred_of_succ, reply_to) -> {
          let assert Ok(succ) = list.first(state.finger_table)
          case pred_of_succ {
            option.None -> {
              process.send(succ.1, RequestPred(reply_to))
              actor.continue(state)
            }
            option.Some(x) -> {
              case x.0 - id > 0 && succ.0 - x.0 >= 0 {
                False -> {
                  io.println(
                    "Node "
                    <> int.to_string(id)
                    <> " stablized! Successor unchanged.",
                  )
                  process.send(succ.1, Notify(reply_to))
                  actor.continue(state)
                }
                True -> {
                  let new_ft = case state.finger_table {
                    [] -> []
                    [_, ..tail] -> [x, ..tail]
                  }
                  io.println(
                    "Node "
                    <> int.to_string(id)
                    <> " stablized! Successor changed. "
                    <> int.to_string(succ.0)
                    <> " -> "
                    <> int.to_string(x.0),
                  )
                  process.send(x.1, Notify(reply_to))
                  actor.continue(NodeState(..state, finger_table: new_ft))
                }
              }
            }
          }
        }

        Notify(node) -> {
          case state.predecessor {
            option.None -> {
              io.println(
                "Node "
                <> int.to_string(id)
                <> " is nodified. Its predecessor is set to node "
                <> int.to_string(node.0),
              )
              actor.continue(NodeState(..state, predecessor: option.Some(node)))
            }
            option.Some(predecessor) -> {
              case predecessor.0 - node.0 < 0 || state.id - node.0 < 0 {
                True -> {
                  io.println(
                    "Node "
                    <> int.to_string(id)
                    <> " is nodified. Its predecessor is set to node "
                    <> int.to_string(node.0),
                  )
                  actor.continue(
                    NodeState(..state, predecessor: option.Some(node)),
                  )
                }
                False -> {
                  io.println(
                    "Node "
                    <> int.to_string(id)
                    <> " is nodified. Its predecessor does not change.",
                  )
                  actor.continue(state)
                }
              }
            }
          }
        }

        AddKey(key) -> {
          actor.continue(
            NodeState(..state, file_keys: [key, ..state.file_keys]),
          )
        }

        RequestID(reply_to, node) -> {
          process.send(reply_to.1, SetPredecessor(#(id, node)))
          actor.continue(state)
        }

        RequestPred(reply_to) -> {
          // let assert Ok(rslt) = list.first(finger_table)
          process.send(reply_to.1, Stablize(state.predecessor, reply_to))
          actor.continue(state)
        }
      }
    })

  let assert Ok(started) = actor.start(builder)
  started.data
}

fn gen_rand(i, t, m, l) {
  // generate t random integers between 0 and m
  case i > t {
    True -> l
    False -> gen_rand(i + 1, t, m, [int.random(m), ..l])
  }
}

fn create_nodes(ids, nodes) {
  case ids {
    [] -> nodes
    [head, ..tail] -> {
      let node = start_node(head)
      create_nodes(tail, [#(head, node), ..nodes])
    }
  }
}

fn set_finger_table(
  id,
  i,
  m,
  finger_table: List(#(Int, process.Subject(NodeMsg))),
  nodes: List(#(Int, process.Subject(NodeMsg))),
) {
  // set finger table of node with id=id
  let assert Ok(max) = int.power(2, 30.0)
  case i >= m {
    True -> {
      // you are finished looking for jumps
      // send the table to the node now
      let assert Ok(sub) = list.key_find(nodes, id)
      // echo id
      // echo finger_table
      case list.reverse(finger_table) {
        [head, ..] -> process.send(head.1, SetPredecessor(#(id, sub)))
        [] -> Nil
      }
      process.send(sub, SetFingers(list.reverse(finger_table)))
      // echo finger_table
      // process.send(list.last(finger_table).1, SetPredecessor(#(id, sub)))
    }
    False -> {
      // exponential jump
      let assert Ok(jump) = int.power(2, int.to_float(i))
      // find first node greater than id + jump
      let rslt = case
        list.first(
          list.filter(nodes, fn(element) {
            element.0 > { id + float.round(jump) } % float.round(max)
          }),
        )
      {
        Ok(rslt) -> rslt
        Error(_) -> {
          // if there is no node greater than id + jump, that means the next one is the first node
          let assert Ok(rslt) = list.first(nodes)
          rslt
        }
      }
      // only add fingers that are unique
      case list.contains(finger_table, rslt) {
        True -> set_finger_table(id, i + 1, m, finger_table, nodes)
        False -> set_finger_table(id, i + 1, m, [rslt, ..finger_table], nodes)
      }
    }
  }
}

fn set_tables(ids, nodes) {
  // set finger tables for each node.
  case ids {
    [] -> Nil
    [head, ..tail] -> {
      set_finger_table(head, 0, 30, [], nodes)
      set_tables(tail, nodes)
    }
  }
}

// assign keys to nodes
fn add_keys(keys, nodes: List(#(Int, process.Subject(NodeMsg)))) {
  case nodes {
    [x, y, ..tail] -> {
      // find keys between x and y
      let filt_keys = list.filter(keys, fn(key) { key > x.0 && key < y.0 })
      // assign those keys to y
      process.send(y.1, SetKeys(filt_keys))
      // assign keys to y
      add_keys(keys, [y, ..tail])
    }
    _ -> Nil
    // you are looking to assign keys to the node after the greatest node. This has already been done
  }
}

fn make_ring(n: Int, k: Int) {
  let assert Ok(m) = int.power(2, 30.0)
  // generate k random integers on range m as the keys
  let keys = gen_rand(1, k, float.round(m), [])
  // sort them for easier assignment
  let keys = list.sort(keys, by: int.compare)
  // generate ids for the nodes
  let ids = gen_rand(1, n, float.round(m), [])
  let ids = list.sort(ids, by: int.compare)
  echo ids
  // create actors
  let nodes = list.reverse(create_nodes(ids, []))
  // create their routing/finger tables
  set_tables(ids, nodes)

  // assign keys to the first node
  let assert Ok(x) = list.first(nodes)
  let assert Ok(y) = list.last(nodes)
  // keys greater than the first node and less than the last node
  let filt_keys = list.filter(keys, fn(key) { key < x.0 || key > y.0 })
  // get stored in the first node
  process.send(x.1, SetKeys(filt_keys))

  // assign keys to all the other nodes
  add_keys(keys, nodes)
  nodes
}

pub fn main() {
  let nodes = make_ring(128, 512)
  let assert Ok(n) = list.first(nodes)
  echo n.0
  // process.send(n.1, FindSuccessor(300_000_000, n.1, False))
  // process.sleep(1000)
  // process.send(n.1, FindSuccessor(10, n.1, False))
  // process.sleep(1000)

  let assert Ok(m) = int.power(2, 30.0)
  let new_id = int.random(float.round(m))
  let new_node = start_node(new_id)

  process.send(n.1, Join(new_id, n.1, new_node))
  process.sleep(1000)
  process.send(n.1, Stablize(option.None, n))
  process.sleep(1000)

  let new_node_2 = start_node(n.0 + 1)
  process.send(n.1, Join(n.0 + 1, n.1, new_node_2))
  process.sleep(1000)
  process.send(n.1, Stablize(option.None, n))
  process.sleep(1000)
}
