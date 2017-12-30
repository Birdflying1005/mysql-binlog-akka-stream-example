package io.wonder.soft.mbs.streams

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage._
import com.github.shyiko.mysql.binlog.event.{Event => BinLogEvent}

class BinLogSource extends GraphStage[SourceShape[BinLogEvent]] {

  val out: Outlet[BinLogEvent] = Outlet("BinLogSource.out")
  override val shape: SourceShape[BinLogEvent] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (BinLogQue.que.nonEmpty) {
            push(out, BinLogQue.que.head)
          }
        }
      })

      override def preStart(): Unit = {
        super.preStart()
      }
    }
  }
}
