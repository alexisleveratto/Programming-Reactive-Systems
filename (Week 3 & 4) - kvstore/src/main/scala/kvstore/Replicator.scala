package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import kvstore.Replica.Remove

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  override def preStart(): Unit = {
    context.system.scheduler.schedule(200 millis, 100 millis)(resend())
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case msg@Replicate(key: String, valueOption: Option[String], id: Long) =>
      val seq = nextSeq()
      replica ! Snapshot(key, valueOption, seq)
      acks += seq -> (sender, msg)

    case msg@SnapshotAck(key: String, seq: Long) =>
      acks.get(seq).foreach { pair =>
        val (originalSender, replicate) = pair
        originalSender ! Replicated(replicate.key, replicate.id)
      }
      acks -= seq
  }

  def resend() = acks.foreach {
    case (seq, (_, r)) =>
      replica ! Snapshot(r.key, r.valueOption, seq)
  }
}
