/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import java.security.KeyStore.TrustedCertificateEntry

import akka.actor._
import akka.event.LoggingReceive

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive =  {
    case Insert(requester: ActorRef, id: Int, elem: Int) =>
      root ! Insert(requester, id, elem)
    case Remove(requester: ActorRef, id: Int, elem: Int) =>
      root ! Remove(requester, id, elem)
    case Contains(requester, id, elem) =>
      root ! Contains(requester, id, elem)
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot), false)
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
      /*
      root = newRoot
      while(!pendingQueue.isEmpty) {
        val (ops, newQueue) = pendingQueue.dequeue
        newRoot ! ops
        pendingQueue = newQueue
      }
      context.become(normal)
       */
      pendingQueue.foreach(newRoot ! _)
      pendingQueue = Queue.empty
      root = newRoot
      context.unbecome()

    case GC => /* ignore GC while garbage collection */
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor  {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive =  {
    case Insert(requester: ActorRef, id: Int, element: Int) => {
      if (element == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (element < elem) {
          subtrees.get(Left) match {
            case Some(leftTree) => leftTree ! Insert(requester, id, element)
            case _ =>
              val leftNode = context.actorOf(props(element, false))
              subtrees += (Left -> leftNode)
              requester ! OperationFinished(id)
          }
      } else {
        subtrees.get(Right) match {
          case Some(rightTree) => rightTree ! Insert(requester, id, element)
          case _ =>
            val rightTree = context.actorOf(props(element, false))
            subtrees += (Right -> rightTree)
            requester ! OperationFinished(id)
        }
      }
    }
    case Contains(requester: ActorRef, id: Int, element: Int) => {
      if (element == elem) {
        requester ! ContainsResult(id, !removed)
      } else if (element < elem) {
        subtrees.get(Left) match {
          case Some(leftTree) => leftTree ! Contains(requester, id, element)
          case _ => requester ! ContainsResult(id, false)
        }
      } else {
        subtrees.get(Right) match {
          case Some(rightTree) => rightTree ! Contains(requester, id, element)
          case _ => requester ! ContainsResult(id, false)
        }
      }
    }
    case Remove(requester: ActorRef, id: Int, element: Int) => {
      if (element == elem){
        if (!removed) {
          removed = true
        }
        requester ! OperationFinished(id)
      } else if (element < elem) {
        subtrees.get(Left) match {
          case Some(leftTree) => leftTree ! Remove(requester, id, element)
          case _ => requester ! OperationFinished(id)
        }
      } else {
        subtrees.get(Right) match {
            case Some(rightTree) => rightTree ! Remove(requester, id, element)
            case _ => requester ! OperationFinished(id)
          }
        }
    }
    case CopyTo(treeNode) => {
      val children = subtrees.values.toSet
      context.become(copying(children, false))

      //root 0 is always removed, but still need to reply OperationFinished(-1)
      if (elem == 0 && removed) {
        self ! OperationFinished(-1)
      } else if (!removed) {
        treeNode ! Insert(self, -1, elem)
      }
      children.foreach(_ ! CopyTo(treeNode))
      /*
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        var expected = Set[ActorRef]()
        if (subtrees contains Left) {
          expected += subtrees(Left)
        }
        if (subtrees contains(Right)) {
          expected += subtrees(Right)
        }

        if (removed){
          context.become(copying(expected, true))
        } else {
          context.become(copying(expected, false))
          treeNode ! Insert(self, 0, elem)
        }
        subtrees.values foreach(_ ! CopyTo(treeNode))
      }
      */
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive =  {
    case OperationFinished(-1) => {
      if (expected.isEmpty) {  //both copy self and copy children is done
        context.parent ! CopyFinished
        self ! PoisonPill
      } else { //still waiting on copying children, but copy self is done
        context.become(copying(expected, true))
      }
    }

    case CopyFinished => {
      val waiting = expected - sender
      if (waiting.isEmpty && insertConfirmed) { //both copy self and copy children is done
        context.parent ! CopyFinished
        self ! PoisonPill
      } else { // waiting on children copy to be done
        context.become(copying(waiting, insertConfirmed))
      }
    }
  }


}
