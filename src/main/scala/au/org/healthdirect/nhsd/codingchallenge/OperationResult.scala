package au.org.healthdirect.nhsd.codingchallenge

sealed trait OperationResult[+A] {

  def map[B](f: A => B): OperationResult[B] =
    flatMap(a => OperationResult.Success(f(a)))

  def flatMap[B](f: A => OperationResult[B]): OperationResult[B] =
    fold(OperationResult.failure, f)

  def fold[B](failure: String => B, success: A => B): B =
    this match {
      case OperationResult.Success(value) => success(value)
      case OperationResult.Failure(error) => failure(error)
    }
}

object OperationResult {
  case class Success[A](value: A) extends OperationResult[A]
  case class Failure(error: String) extends OperationResult[Nothing]

  def success[A](value: A): OperationResult[A] = Success(value)

  def failure(error: String): OperationResult[Nothing] = Failure(error)
}
