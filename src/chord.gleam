import gleam/erlang/process
import gleam/int
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

pub type NodeMsg {
  // Find the successor of id
  FindSuccessor(id: Int, process.Subject(NodeMsg))

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
