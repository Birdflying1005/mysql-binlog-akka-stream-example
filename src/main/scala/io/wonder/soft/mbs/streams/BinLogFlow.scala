package io.wonder.soft.mbs.streams

import akka.stream.stage._
import akka.stream._
import com.github.shyiko.mysql.binlog.event.{Event => BinLogEvent}

class BinLogFlow extends GraphStage[FlowShape[BinLogEvent, String]] {
  val in: Inlet[BinLogEvent] = Inlet("BinLogSink.in")
  val out: Outlet[String] = Outlet("BinLogSink.out")

  override val shape: FlowShape[BinLogEvent, String] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      // This requests one element at the Sink startup.
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          pull(in)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (BinLogQue.que.nonEmpty) {
            push(out, BinLogQue.que.head.getData.toString)
            BinLogQue.que.dequeue()
          }
        }
      })
    }
  }
}
