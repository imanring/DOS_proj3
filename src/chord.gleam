import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor

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
          case range_in(finger_id, n, id) {
            True -> Ok(head)
            False -> find_closest_preceding_finger(n, id, tail)
          }
        }
      }
    }
  }
}

// is x in (a,b)
fn range_in(x, a, b) {
  { { { a < x } && { x < b } } || { { a > b } && { { a < x } || { x < b } } } }
}

pub type RequestType {
  SetSuccessor
  FindFileKey
  ReplaceFinger(id: Int)
}

pub type NodeMsg {
  // Find the successor of id
  FindSuccessor(id: Int, reply_to: process.Subject(NodeMsg))
  // process the result of finding the successor of id
  SuccessorResult(result: #(Int, Int, process.Subject(NodeMsg)))
  // verifies the successor and tells the successor about itself
  Stablize(
    pred_of_succ: option.Option(#(Int, process.Subject(NodeMsg))),
    reply_to: #(Int, process.Subject(NodeMsg)),
  )
  // join the network by asking an existing node to find your successor
  Join(node: process.Subject(NodeMsg))
  // periodically verify the finger table
  FixFingers(self_subject: process.Subject(NodeMsg))
  // notify a node that you might be its predecessor
  Notify(node: #(Int, process.Subject(NodeMsg)))
  // set the finger table
  SetFingers(finger_table: List(#(Int, process.Subject(NodeMsg))))
  // set file keys table
  SetKeys(keys: List(Int))
  AddKey(key: Int)
  SearchFileKey(key: Int, self_subject: process.Subject(NodeMsg))
  SetPredecessor(#(Int, process.Subject(NodeMsg)))
  // Ask the predecessor to send its id and then set predecessor
  RequestPred(reply_to: #(Int, process.Subject(NodeMsg)))
  ShutDown
}

pub type NodeState {
  NodeState(
    id: Int,
    file_keys: List(Int),
    // finger table should be sorted from closest to furthest in a clockwise manner
    predecessor: option.Option(#(Int, process.Subject(NodeMsg))),
    finger_table: List(#(Int, process.Subject(NodeMsg))),
    requests_table: List(#(Int, RequestType)),
    //    boss: process.Subject(BossMsg),
  )
}

fn fix_fingers(i: Int, self_subject: process.Subject(NodeMsg), state: NodeState) {
  // check the n + 2^i for a next finger
  let assert Ok(m) = int.power(2, 30.0)
  let m = float.round(m)
  let assert Ok(jump) = int.power(2, int.to_float(i))
  let id = { state.id + float.round(jump) } % m
  process.send(self_subject, FindSuccessor(id, self_subject))
  // add to requests table
  // only replace finger if it is between self_id + 2^i % m and self_id + 2^(i+1) % m
  let new_request = case
    list.filter(state.finger_table, fn(x) {
      x.0 > id && x.0 < { state.id + float.round(jump *. 2.0) } % m
    })
  {
    [] -> #(id, ReplaceFinger(-1))
    [head, ..] -> #(id, ReplaceFinger(head.0))
  }
  case i >= 29 {
    True -> {
      [new_request]
    }
    False -> {
      let other_requests = fix_fingers(i + 1, self_subject, state)
      [new_request, ..other_requests]
    }
  }
}

fn handle_messages(state: NodeState, msg: NodeMsg) {
  // handle messages
  case msg {
    ShutDown -> actor.stop()

    SetFingers(finger_table) -> {
      // sort fingers
      let assert Ok(m) = int.power(2, 30.0)
      let m = float.round(m)
      let finger_table =
        list.sort(finger_table, fn(a, b) {
          int.compare({ a.0 + m - state.id } % m, { b.0 + m - state.id } % m)
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

    SuccessorResult(result) -> {
      //io.println("Got result! The Id is " <> int.to_string(result.0))
      case list.key_find(state.requests_table, result.0) {
        Ok(rslt) -> {
          // remove from requests table
          let new_request_table =
            list.filter(state.requests_table, fn(x) { x != #(result.0, rslt) })
          case rslt {
            SetSuccessor -> {
              // set successor to result
              let new_finger_table = case state.finger_table {
                [] -> [#(result.1, result.2)]
                [_first, ..rest] -> [#(result.1, result.2), ..rest]
              }
              // remove from requests table and set successor
              actor.continue(
                NodeState(
                  ..state,
                  requests_table: new_request_table,
                  finger_table: new_finger_table,
                ),
              )
            }
            FindFileKey -> {
              io.println(
                "Node "
                <> int.to_string(state.id)
                <> " received the file key "
                <> int.to_string(result.0)
                <> ".",
              )
              // remove from requests table
              actor.continue(
                NodeState(..state, requests_table: new_request_table),
              )
            }
            ReplaceFinger(finger_id) -> {
              // replace finger with result
              let new_finger_table = case finger_id {
                -1 -> {
                  // add it to the table if it isn't already there
                  case
                    list.contains(state.finger_table, #(result.1, result.2))
                  {
                    True -> state.finger_table
                    False -> {
                      io.println(
                        "Adding to Node "
                        <> int.to_string(state.id)
                        <> "'s finger table.",
                      )
                      [#(result.1, result.2), ..state.finger_table]
                    }
                  }
                }
                _ -> {
                  io.println(
                    "Node "
                    <> int.to_string(state.id)
                    <> " replacing finger "
                    <> int.to_string(finger_id)
                    <> " with "
                    <> int.to_string(result.1)
                    <> ".",
                  )
                  // replace the finger with finger_id
                  list.map(state.finger_table, fn(x) {
                    case x.0 == finger_id {
                      True -> #(result.1, result.2)
                      False -> x
                    }
                  })
                }
              }
              // sort fingers
              let assert Ok(m) = int.power(2, 30.0)
              let m = float.round(m)
              let new_finger_table =
                list.sort(new_finger_table, fn(a, b) {
                  int.compare(
                    { a.0 + m - state.id } % m,
                    { b.0 + m - state.id } % m,
                  )
                })
              // echo list.unzip(new_finger_table).0
              // remove from requests table and set successor
              actor.continue(
                NodeState(
                  ..state,
                  requests_table: new_request_table,
                  finger_table: new_finger_table,
                ),
              )
            }
          }
        }
        _ -> {
          io.println("I didn't know about that request!")
          actor.continue(state)
        }
      }
    }

    FindSuccessor(id, reply_to) -> {
      let assert Ok(successor) = list.first(state.finger_table)
      let found = range_in(id, state.id, successor.0) || id == successor.0
      case found {
        True -> {
          //io.println(
          //  "node "
          //  <> int.to_string(state.id)
          //  <> " found successor of "
          //  <> int.to_string(id),
          //)
          // send successor to reply_to
          process.send(
            reply_to,
            SuccessorResult(#(id, successor.0, successor.1)),
          )
          actor.continue(state)
        }
        False -> {
          let assert Ok(closest_preceding_finger) =
            find_closest_preceding_finger(
              state.id,
              id,
              list.reverse(state.finger_table),
            )
          //io.println(
          //  "node "
          //  <> int.to_string(state.id)
          //  <> " forwarding to finger "
          //  <> int.to_string(closest_preceding_finger.0),
          //)
          // forward the message to closest_preceding_finger
          process.send(closest_preceding_finger.1, FindSuccessor(id, reply_to))
          actor.continue(state)
        }
      }
    }

    Stablize(pred_of_succ, reply_to) -> {
      let assert Ok(succ) = list.first(state.finger_table)
      case pred_of_succ {
        option.None -> {
          process.send(succ.1, RequestPred(reply_to))
          actor.continue(state)
        }
        option.Some(x) -> {
          case range_in(x.0, state.id, succ.0) {
            False -> {
              io.println(
                "Node "
                <> int.to_string(state.id)
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
                <> int.to_string(state.id)
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
            <> int.to_string(state.id)
            <> " is nodified. Its predecessor is set to node "
            <> int.to_string(node.0),
          )
          actor.continue(NodeState(..state, predecessor: option.Some(node)))
        }
        option.Some(predecessor) -> {
          case range_in(node.0, predecessor.0, state.id) {
            True -> {
              io.println(
                "Node "
                <> int.to_string(state.id)
                <> " is nodified. Its predecessor is set to node "
                <> int.to_string(node.0),
              )
              actor.continue(NodeState(..state, predecessor: option.Some(node)))
            }
            False -> {
              io.println(
                "Node "
                <> int.to_string(state.id)
                <> " is nodified. Its predecessor does not change.",
              )
              actor.continue(state)
            }
          }
        }
      }
    }

    AddKey(key) -> {
      actor.continue(NodeState(..state, file_keys: [key, ..state.file_keys]))
    }

    Join(node) -> {
      process.send(node, FindSuccessor(state.id, node))
      actor.continue(
        NodeState(..state, requests_table: [
          #(state.id, SetSuccessor),
          ..state.requests_table
        ]),
      )
    }

    SearchFileKey(key, self_subject) -> {
      // find the successor of key
      process.send(self_subject, FindSuccessor(key, self_subject))
      actor.continue(
        NodeState(..state, requests_table: [
          #(key, FindFileKey),
          ..state.requests_table
        ]),
      )
    }

    FixFingers(self_subject) -> {
      // fix fingers
      let new_requests = fix_fingers(0, self_subject, state)
      actor.continue(
        NodeState(
          ..state,
          requests_table: list.append(new_requests, state.requests_table),
        ),
      )
    }

    RequestPred(reply_to) -> {
      // let assert Ok(rslt) = list.first(finger_table)
      process.send(reply_to.1, Stablize(state.predecessor, reply_to))
      actor.continue(state)
    }
  }
}

fn start_node(id: Int) -> process.Subject(NodeMsg) {
  let init = NodeState(id, [], option.None, [], [])

  let builder =
    actor.new(init)
    |> actor.on_message(handle_messages)

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
  // echo ids
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
  let assert Ok(m) = int.power(2, 30.0)
  let new_id = int.random(float.round(m))
  let new_node = start_node(new_id)
  process.send(new_node, Join(n.1))
  process.send(new_node, Stablize(option.None, #(new_id, new_node)))
  process.send(n.1, FixFingers(n.1))
  // echo n.0
  process.send(n.1, SearchFileKey(300_000_000, n.1))
  process.sleep(1000)
  process.send(n.1, SearchFileKey(1000, n.1))
  process.sleep(1000)
}
