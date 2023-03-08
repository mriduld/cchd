package au.org.healthdirect.nhsd.codingchallenge

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EventHandlerSpec extends FunSuite {
  test("Validate Name"){
    assert(
      EventHandler.validateName("  ") === OperationResult.failure("Missing name for Organisation payload")
    )
  }
  test("Validate Service Type"){
    assert(EventHandler.validateServiceType("Pharmacy") === OperationResult.success(()))
    assert(EventHandler.validateServiceType("GP") === OperationResult.success(()))
    assert(EventHandler.validateServiceType("Test") ===
      OperationResult.failure("Invalid serviceType: 'Test'")
    )
  }
  test("Validate Address"){
    val address = Address(
      "1 Street",
      "Sydney",
      "NSW",
      "2000"
    )
    assert(
      EventHandler.validateAddress(address) == OperationResult.success(())
    )
    assert(
      EventHandler.validateAddress(address.copy(street = "  ")) ===
      OperationResult.failure("Missing street for address")
    )
    assert(
      EventHandler.validateAddress(address.copy(suburb = "  ")) ===
        OperationResult.failure("Missing suburb for address")
    )
    assert(
      EventHandler.validateAddress(address.copy(state = "  ")) ===
        OperationResult.failure("Missing state for address")
    )
    assert(
      EventHandler.validateAddress(address.copy(postcode = "  ")) ===
        OperationResult.failure("Missing postcode for address")
    )
    assert(
      EventHandler.validateAddress(address.copy(postcode = "abcd")) ===
        OperationResult.failure("Postcode should be 4 digit number")
    )
    assert(
      EventHandler.validateAddress(address.copy(postcode = "12345")) ===
        OperationResult.failure("Postcode should be 4 digit number")
    )
  }
}
