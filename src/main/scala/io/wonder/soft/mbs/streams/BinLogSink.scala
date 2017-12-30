package io.wonder.soft.mbs.streams

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}

class BinLogSink extends GraphStage[SinkShape[String]] {
  val in: Inlet[String] = Inlet("BinLogSink.in")
  override val shape: SinkShape[String] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val event = grab(in)
          println(s"que_size: ${BinLogQue.que.size}, event: ${event}")

          // want to change this thread waiting...
          while (BinLogQue.que.isEmpty) {
            Thread.sleep(1000L)
          }

          pull(in)
        }
      })
    }
  }
}
