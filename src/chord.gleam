import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
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
      let #(finger_id, finger_node) = head
      case list.length(tail) {
        0 -> Ok(head)
        _ -> {
          // if finger_id is between n and id mod 2^m, return finger_node
          case
            { { id > finger_id } && { n < finger_id } }
            || { { n > id } && { { id > finger_id } || { n < finger_id } } }
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
  FindSuccessor(id: Int, reply_to: process.Subject(NodeMsg))
  SuccessorResult(result: #(Int, process.Subject(NodeMsg)))
  SetFingers(finger_table: List(#(Int, process.Subject(NodeMsg))))
  SetKeys(keys: List(Int))
  AddKey(key: Int)
  // Notify(process.Subject(NodeMsg))
  // GetPredecessor(process.Subject(NodeMsg))
  // PredecessorResponse(Maybe(process.Subject(NodeMsg)))
  ShutDown
}

pub type NodeState {
  NodeState(
    id: Int,
    file_keys: List(Int),
    finger_table: List(#(Int, process.Subject(NodeMsg))),
    requests_table: List(Int),
    //    boss: process.Subject(BossMsg),
  )
}

fn start_node(id: Int) -> process.Subject(NodeMsg) {
  let init = NodeState(id, [], [], [])

  let builder =
    actor.new(init)
    |> actor.on_message(fn(state, msg) {
      case msg {
        ShutDown -> actor.stop()

        SetFingers(finger_table) -> {
          io.println(
            "node "
            <> int.to_string(state.id)
            <> " fingers set: "
            <> int.to_string(list.length(finger_table)),
          )
          echo list.unzip(finger_table).0
          //echo finger_table
          //io.debug(finger_table)
          actor.continue(NodeState(..state, finger_table: finger_table))
        }

        SetKeys(keys) -> {
          actor.continue(NodeState(..state, file_keys: keys))
        }
        SuccessorResult(result) -> {
          io.println("Got result! The Id is " <> int.to_string(result.0))
          actor.continue(state)
        }

        FindSuccessor(id, reply_to) -> {
          io.println(
            "node "
            <> int.to_string(state.id)
            <> " finding successor of "
            <> int.to_string(id),
          )
          let assert Ok(successor) = list.first(state.finger_table)
          let found =
            { { id > state.id } && { id < successor.0 } }
            || {
              { state.id > id }
              && { { id > successor.0 } || { state.id < successor.0 } }
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
              process.send(reply_to, SuccessorResult(successor))
              actor.continue(state)
            }
            False -> {
              let assert Ok(closest_preceding_finger) =
                find_closest_preceding_finger(state.id, id, state.finger_table)
              io.println(
                "node "
                <> int.to_string(state.id)
                <> " forwarding to finger"
                <> int.to_string(closest_preceding_finger.0),
              )
              // forward the message to closest_preceding_finger
              // let result = process.call(closest_preceding_finger)
              process.sleep(100)
              process.send(
                closest_preceding_finger.1,
                FindSuccessor(id, reply_to),
              )
              actor.continue(state)
            }
          }
        }

        AddKey(key) -> {
          actor.continue(
            NodeState(..state, file_keys: [key, ..state.file_keys]),
          )
        }
      }
    })

  let assert Ok(started) = actor.start(builder)
  started.data
}

fn gen_rand(i, t, m, l) {
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
  finger_table,
  nodes: List(#(Int, process.Subject(NodeMsg))),
) {
  let assert Ok(max) = int.power(2, 30.0)
  case i >= m {
    True -> {
      let assert Ok(sub) = list.key_find(nodes, id)
      process.send(sub, SetFingers(finger_table))
    }
    False -> {
      let assert Ok(jump) = int.power(2, int.to_float(i))
      let rslt = case
        list.first(
          list.filter(nodes, fn(element) {
            element.0 > { id + float.round(jump) } % float.round(max)
          }),
        )
      {
        Ok(rslt) -> rslt
        Error(_) -> {
          let assert Ok(rslt) = list.first(nodes)
          rslt
        }
      }
      case list.contains(finger_table, rslt) {
        True -> set_finger_table(id, i + 1, m, finger_table, nodes)
        False -> set_finger_table(id, i + 1, m, [rslt, ..finger_table], nodes)
      }
    }
  }
}

fn set_tables(ids, nodes) {
  case ids {
    [] -> Nil
    [head, ..tail] -> {
      set_finger_table(head, 0, 30, [], nodes)
      set_tables(tail, nodes)
    }
  }
}

fn add_keys(keys, nodes: List(#(Int, process.Subject(NodeMsg)))) {
  case nodes {
    [x, y, ..tail] -> {
      let filt_keys = list.filter(keys, fn(key) { key > x.0 && key < y.0 })
      process.send(y.1, SetKeys(filt_keys))
      add_keys(keys, tail)
    }
    _ -> Nil
  }
}

fn make_ring(n: Int, k: Int) {
  let assert Ok(m) = int.power(2, 30.0)
  // generate k random integers on range m
  let keys = gen_rand(1, k, float.round(m), [])
  let keys = list.sort(keys, by: int.compare)
  let ids = gen_rand(1, n, float.round(m), [])
  let ids = list.sort(ids, by: int.compare)
  echo ids
  let nodes = list.reverse(create_nodes(ids, []))
  set_tables(ids, nodes)
  add_keys(keys, nodes)
  nodes
}

pub fn main() {
  let nodes = make_ring(16, 512)
  let assert Ok(n) = list.first(nodes)
  echo n.0
  process.send(n.1, FindSuccessor(100_000_000, n.1))
  process.sleep(1000)
}
