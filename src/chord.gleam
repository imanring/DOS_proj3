import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor

// import gleam/crypto

fn closest_preceding_finger(
  n: Int,
  id: Int,
  finger_table: List(#(Int, process.Subject(NodeMsg))),
) {
  case finger_table {
    [] -> Nil
    [head, ..tail] -> {
      let #(finger_id, finger_node) = head
      case list.length(tail) {
        0 -> finger_node
        _ -> {
          // if finger_id is between n and id mod 2^m, return finger_node
          case
            { { id > finger_id } && { n < finger_id } }
            || { { n > id } && { { id > finger_id } || { n < finger_id } } }
          {
            True -> finger_node
            False -> closest_preceding_finger(n, id, tail)
          }
        }
      }
    }
  }
}

pub type NodeMsg {
  // Find the successor of id
  FindSuccessor(id: Int, process.Subject(NodeMsg))
  SetFingers(finger_table: List(#(Int, process.Subject(NodeMsg))))
  // Notify(process.Subject(NodeMsg))
  // GetPredecessor(process.Subject(NodeMsg))
  // PredecessorResponse(Maybe(process.Subject(NodeMsg)))
  ShutDown
}

type NodeState {
  NodeState(
    id: Int,
    file_keys: List(Int),
    finger_table: List(#(Int, process.Subject(NodeMsg))),
    boss: process.Subject(BossMsg),
  )
}

fn start_node(
  id: Int,
  file_keys: List(Int),
  boss: process.Subject(BossMsg),
) -> process.Subject(NodeMsg) {
  let init = NodeState(id, file_keys, [], boss)

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
          actor.continue(NodeState(..state, finger_table: finger_table))
        }

        FindSuccessor(id, reply_to) -> {
          io.println(
            "node "
            <> int.to_string(state.id)
            <> " finding successor of "
            <> int.to_string(id),
          )
          let found =
            { { id > state.id } && { id < first(state.finger_table).0 } }
            || {
              { state.id > id }
              && {
                { id > first(state.finger_table).0 }
                || { state.id < first(state.finger_table).0 }
              }
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
              actor.continue(state)
            }
            False -> {
              let closest_preceding_finger =
                closest_preceding_finger(state.id, id, state.finger_table)
              io.println(
                "node " <> int.to_string(state.id) <> " forwarding to finger",
              )
              // forward the message to closest_preceding_finger
              actor.continue(state)
            }
          }
        }
      }
    })

  let assert Ok(started) = actor.start(builder)
  started.data
}
