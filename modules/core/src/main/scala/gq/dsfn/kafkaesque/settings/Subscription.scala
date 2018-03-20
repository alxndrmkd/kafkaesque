package gq.dsfn.kafkaesque.settings

import org.apache.kafka.common.TopicPartition

sealed trait Offset

final case class SimpleOffset(value: Long) extends Offset

final case class TimestampOffset(timestamp: Long) extends Offset

final case class Subscription(topics: Set[String] = Set.empty,
                              assignments: Set[TopicPartition] = Set.empty,
                              assignmentsWithOffset: Map[TopicPartition, Offset] = Map.empty) {

  private val topicSet = topics
    .intersect(assignments.map(_.topic()))
    .intersect(assignmentsWithOffset.keySet.map(_.topic()))

  def put(topic: String): Subscription = {
    copy(topics = topics + topic)
  }

  def put(assignment: TopicPartition): Subscription = {
    copy(assignments = assignments + assignment)
  }

  def put(assignmentWithOffset: (TopicPartition, Offset)): Subscription = {
    copy(assignmentsWithOffset = assignmentsWithOffset + assignmentWithOffset)
  }
}
