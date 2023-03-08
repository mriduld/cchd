package au.org.healthdirect.nhsd.codingchallenge


import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class OperationResultSpec extends FunSuite {
  def f(a: Int): OperationResult[Int] = OperationResult.success(a + 1)
  def g(a: Int): OperationResult[Int] = OperationResult.success(a + 2)

  test("Left Identity"){
    assert(
      OperationResult.success(1).flatMap(f) === f(1)
    )
  }
  test("Right Identity") {
    assert(
      OperationResult.success(1).flatMap(OperationResult.success) === OperationResult.success(1)
    )
  }
  test("Associativity") {
    assert(
      OperationResult.success(1).flatMap(f).flatMap(g) ===
        OperationResult.success(1).flatMap(f(_).flatMap(g))
    )
  }
  test("Map Failure") {
    assert(
      OperationResult.failure("test").map(_ => 1) === OperationResult.failure("test")
    )
  }
  test("Map Success") {
    assert(
      OperationResult.success(1).map(_ + 1) === OperationResult.success(2)
    )
  }
  test("Fold Failure") {
    assert(
      OperationResult.failure("test").fold(s => s"failed $s", s => s"passed $s") === "failed test"
    )
  }
  test("Fold Success") {
    assert(
      OperationResult.success("test").fold(s => s"failed $s", s => s"passed $s") === "passed test"
    )
  }
}
